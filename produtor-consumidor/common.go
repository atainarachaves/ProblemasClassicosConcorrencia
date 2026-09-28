package main

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// tempoDeTrabalho simula o custo de "produzir" ou "processar" um item, para
// que throughput e ocupação do buffer sejam observáveis (sem isso, a
// simulação é tão rápida que o buffer nunca chega a ocupar nem 1 item).
func tempoDeTrabalho(rng *rand.Rand, minUs, maxUs int) time.Duration {
	return time.Duration(minUs+rng.Intn(maxUs-minUs+1)) * time.Microsecond
}

// Item é a unidade transportada pelo buffer. `Poison` marca um item
// sentinela ("poison pill"): sinaliza a um consumidor específico que não
// virão mais itens, permitindo encerramento ordenado mesmo na versão com
// semáforos, que não tem um equivalente a `close(channel)`.
type Item struct {
	ProdutorID int
	Seq        int
	Poison     bool
}

// Config descreve uma execução da simulação produtor/consumidor.
type Config struct {
	Impl             string // "channel" ou "semaforo"
	P, C, K          int
	ItensPorProdutor int
	TimeoutConsumidor time.Duration
	AmostragemMs      int
}

// Resultado agrega as métricas coletadas em uma execução.
type Resultado struct {
	Config           Config
	Produzido        int64
	Consumido        int64
	ItensPorConsumo  []int64
	OcupacaoMedia    float64
	OcupacaoMax      int64
	Duracao          time.Duration
	IdleConsumidor0  int64 // quantas vezes o consumidor-sentinela disparou o timeout
}

func (r Resultado) Throughput() float64 {
	if r.Duracao <= 0 {
		return 0
	}
	return float64(r.Consumido) / r.Duracao.Seconds()
}

// monitorOcupacao amostra periodicamente um contador atômico de ocupação do
// buffer e devolve, ao final (quando `done` é fechado), a média e o máximo
// observados. Roda em goroutine própria; leitura do contador é sempre via
// atomic, nunca direta, pois é escrita concorrentemente por produtores e
// consumidores.
func monitorOcupacao(ocupacaoAtual *int64, intervalo time.Duration, done <-chan struct{}) (media float64, max int64) {
	var soma int64
	var amostras int64
	ticker := time.NewTicker(intervalo)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			if amostras == 0 {
				return 0, max
			}
			return float64(soma) / float64(amostras), max
		case <-ticker.C:
			v := atomic.LoadInt64(ocupacaoAtual)
			soma += v
			amostras++
			if v > max {
				max = v
			}
		}
	}
}
