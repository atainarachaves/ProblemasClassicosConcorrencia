package main

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// simularChannel implementa o buffer limitado usando a semântica nativa de
// channel com buffer: `make(chan Item, K)`. O próprio channel garante
// exclusão mútua e bloqueio quando cheio/vazio — não há necessidade de
// mutex ou semáforo explícitos.
func simularChannel(cfg Config) Resultado {
	buffer := make(chan Item, cfg.K)
	ocupacao := new(int64) // contador atômico só para a métrica de monitoramento

	var wgProdutores sync.WaitGroup
	var wgConsumidores sync.WaitGroup
	itensPorConsumidor := make([]int64, cfg.C)
	var totalProduzido int64

	inicio := time.Now()

	// --- produtores ---
	wgProdutores.Add(cfg.P)
	for p := 0; p < cfg.P; p++ {
		go func(id int) {
			defer wgProdutores.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*7919))
			for s := 0; s < cfg.ItensPorProdutor; s++ {
				time.Sleep(tempoDeTrabalho(rng, 100, 400)) // simula custo de produzir
				buffer <- Item{ProdutorID: id, Seq: s}      // bloqueia se buffer estiver cheio (K itens)
				atomic.AddInt64(&totalProduzido, 1)
				atomic.AddInt64(ocupacao, 1)
			}
		}(p)
	}

	// Só quem produz fecha o channel, e só depois que TODOS os produtores
	// terminaram — fechar mais de uma vez, ou fechar do lado do consumidor,
	// é erro de concorrência em Go (panic).
	go func() {
		wgProdutores.Wait()
		close(buffer)
	}()

	// --- consumidores ---
	var idleConsumidor0 int64
	wgConsumidores.Add(cfg.C)
	for c := 0; c < cfg.C; c++ {
		go func(id int) {
			defer wgConsumidores.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*104729))
			for {
				if id == 0 {
					// Consumidor-sentinela: usa select com time.After para
					// demonstrar o comportamento alternativo exigido pelo
					// enunciado (não fica bloqueado indefinidamente sem
					// diagnosticar buffer ocioso).
					select {
					case _, ok := <-buffer:
						if !ok {
							return // channel fechado e drenado: encerra
						}
						atomic.AddInt64(ocupacao, -1)
						time.Sleep(tempoDeTrabalho(rng, 100, 400)) // simula custo de processar
						atomic.AddInt64(&itensPorConsumidor[id], 1)
					case <-time.After(cfg.TimeoutConsumidor):
						atomic.AddInt64(&idleConsumidor0, 1)
					}
				} else {
					v, ok := <-buffer
					if !ok {
						return
					}
					atomic.AddInt64(ocupacao, -1)
					time.Sleep(tempoDeTrabalho(rng, 100, 400)) // simula custo de processar
					atomic.AddInt64(&itensPorConsumidor[id], 1)
					_ = v
				}
			}
		}(c)
	}

	// --- monitor de ocupação ---
	done := make(chan struct{})
	var media float64
	var max int64
	var wgMonitor sync.WaitGroup
	wgMonitor.Add(1)
	go func() {
		defer wgMonitor.Done()
		media, max = monitorOcupacao(ocupacao, time.Duration(cfg.AmostragemMs)*time.Millisecond, done)
	}()

	wgConsumidores.Wait()
	close(done)
	wgMonitor.Wait()

	duracao := time.Since(inicio)

	var totalConsumido int64
	for _, v := range itensPorConsumidor {
		totalConsumido += v
	}

	return Resultado{
		Config:          cfg,
		Produzido:       atomic.LoadInt64(&totalProduzido),
		Consumido:       totalConsumido,
		ItensPorConsumo: itensPorConsumidor,
		OcupacaoMedia:   media,
		OcupacaoMax:     max,
		Duracao:         duracao,
		IdleConsumidor0: idleConsumidor0,
	}
}
