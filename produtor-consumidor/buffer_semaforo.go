package main

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// simularSemaforo implementa o buffer limitado "na mão": um array circular
// de capacidade K, dois semáforos de contagem (`notEmpty`/`notFull`,
// implementados como channels `chan struct{}` usados apenas como contador —
// nunca como fila de dados) e um `sync.Mutex` para a exclusão mútua da
// região crítica (posições de leitura/escrita do array).
//
// Isso é o que o enunciado pede explicitamente: com múltiplos produtores e
// múltiplos consumidores, os semáforos por si só (notEmpty/notFull) só
// contam vagas/itens — eles NÃO impedem que dois produtores escrevam na
// mesma posição do array ao mesmo tempo, ou que dois consumidores leiam o
// mesmo item. Por isso o mutex é indispensável aqui, mesmo já havendo
// semáforos.
func simularSemaforo(cfg Config) Resultado {
	fila := make([]Item, cfg.K)
	head, tail := 0, 0
	var mu sync.Mutex // protege head, tail e o conteúdo de `fila`

	notFull := make(chan struct{}, cfg.K)  // vagas livres — começa cheio (K vagas)
	notEmpty := make(chan struct{}, cfg.K) // itens disponíveis — começa vazio
	for i := 0; i < cfg.K; i++ {
		notFull <- struct{}{}
	}

	ocupacao := new(int64)

	var wgProdutores sync.WaitGroup
	var wgConsumidores sync.WaitGroup
	itensPorConsumidor := make([]int64, cfg.C)
	var totalProduzido int64

	inicio := time.Now()

	enfileirar := func(it Item) {
		<-notFull // espera vaga (bloqueia se K itens já estão na fila)
		mu.Lock()
		fila[tail] = it
		tail = (tail + 1) % cfg.K
		mu.Unlock()
		notEmpty <- struct{}{} // sinaliza item disponível
	}

	// --- produtores ---
	wgProdutores.Add(cfg.P)
	for p := 0; p < cfg.P; p++ {
		go func(id int) {
			defer wgProdutores.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*7919))
			for s := 0; s < cfg.ItensPorProdutor; s++ {
				time.Sleep(tempoDeTrabalho(rng, 100, 400)) // simula custo de produzir
				enfileirar(Item{ProdutorID: id, Seq: s})
				atomic.AddInt64(&totalProduzido, 1)
				atomic.AddInt64(ocupacao, 1)
			}
		}(p)
	}

	// Encerramento ordenado sem `close`: após todos os produtores
	// terminarem, empilha exatamente C "poison pills" (uma por consumidor).
	// Como a fila é FIFO, elas só são consumidas depois de todo item real
	// já inserido — nenhum item de trabalho é perdido.
	go func() {
		wgProdutores.Wait()
		for i := 0; i < cfg.C; i++ {
			enfileirar(Item{Poison: true})
		}
	}()

	// --- consumidores ---
	var idleConsumidor0 int64
	wgConsumidores.Add(cfg.C)
	for c := 0; c < cfg.C; c++ {
		go func(id int) {
			defer wgConsumidores.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*104729))
			for {
				var v Item
				if id == 0 {
					// Consumidor-sentinela: select com time.After.
					select {
					case <-notEmpty:
						mu.Lock()
						v = fila[head]
						head = (head + 1) % cfg.K
						mu.Unlock()
						notFull <- struct{}{}
					case <-time.After(cfg.TimeoutConsumidor):
						atomic.AddInt64(&idleConsumidor0, 1)
						continue
					}
				} else {
					<-notEmpty
					mu.Lock()
					v = fila[head]
					head = (head + 1) % cfg.K
					mu.Unlock()
					notFull <- struct{}{}
				}

				if v.Poison {
					return
				}
				atomic.AddInt64(ocupacao, -1)
				time.Sleep(tempoDeTrabalho(rng, 100, 400)) // simula custo de processar
				atomic.AddInt64(&itensPorConsumidor[id], 1)
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
