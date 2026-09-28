// Comando: produtor-consumidor
//
// Duas implementações de um buffer limitado (capacidade K) para múltiplos
// produtores e consumidores:
//
//	channel   -> make(chan Item, K), semântica de bloqueio nativa do Go
//	semaforo  -> array circular + semáforos de contagem (notEmpty/notFull,
//	             via channels) + sync.Mutex para exclusão mútua
//
// Uso (execução única):
//
//	go run . -impl channel  -p 3 -c 2 -k 10 -itens 200
//	go run . -impl semaforo -p 3 -c 2 -k 10 -itens 200
//
// Uso (experimento variando K, para o relatório):
//
//	go run . -experimento -p 3 -c 3 -itens 500
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	impl := flag.String("impl", "channel", "implementação: channel | semaforo")
	p := flag.Int("p", 3, "número de produtores")
	c := flag.Int("c", 3, "número de consumidores")
	k := flag.Int("k", 10, "capacidade do buffer")
	itens := flag.Int("itens", 300, "itens produzidos por produtor")
	timeoutMs := flag.Int("timeout", 50, "ms de timeout do consumidor-sentinela (select)")
	amostragemMs := flag.Int("amostragem", 5, "intervalo (ms) de amostragem da ocupação do buffer")
	experimento := flag.Bool("experimento", false, "roda o experimento variando K = 1, 10, 100 para channel e semaforo")
	flag.Parse()

	if *experimento {
		rodarExperimento(*p, *c, *itens, *timeoutMs, *amostragemMs)
		return
	}

	if *impl != "channel" && *impl != "semaforo" {
		fmt.Fprintln(os.Stderr, "impl deve ser: channel ou semaforo")
		os.Exit(1)
	}

	cfg := Config{
		Impl:              *impl,
		P:                 *p,
		C:                 *c,
		K:                 *k,
		ItensPorProdutor:  *itens,
		TimeoutConsumidor: time.Duration(*timeoutMs) * time.Millisecond,
		AmostragemMs:      *amostragemMs,
	}

	res := rodar(cfg)
	imprimirResultado(res)

	if res.Produzido != res.Consumido {
		fmt.Fprintf(os.Stderr, "\nERRO DE INVARIANTE: produzido (%d) != consumido (%d)\n", res.Produzido, res.Consumido)
		os.Exit(1)
	}
}

func rodar(cfg Config) Resultado {
	if cfg.Impl == "semaforo" {
		return simularSemaforo(cfg)
	}
	return simularChannel(cfg)
}

func imprimirResultado(r Resultado) {
	fmt.Printf("=== Produtor/Consumidor — impl: %s (P=%d, C=%d, K=%d, itens/produtor=%d) ===\n",
		r.Config.Impl, r.Config.P, r.Config.C, r.Config.K, r.Config.ItensPorProdutor)
	fmt.Println()
	for i, v := range r.ItensPorConsumo {
		fmt.Printf("Consumidor %d: %d itens\n", i, v)
	}
	fmt.Println()
	fmt.Printf("Produzido: %d | Consumido: %d | OK (produzido == consumido): %v\n",
		r.Produzido, r.Consumido, r.Produzido == r.Consumido)
	fmt.Printf("Duração: %s | Throughput: %.1f itens/s\n", r.Duracao, r.Throughput())
	fmt.Printf("Ocupação média do buffer: %.2f/%d | Ocupação máxima observada: %d/%d\n",
		r.OcupacaoMedia, r.Config.K, r.OcupacaoMax, r.Config.K)
	fmt.Printf("Consumidor-sentinela (id 0) disparou timeout (select) %d vezes\n", r.IdleConsumidor0)
}

// rodarExperimento executa channel e semaforo para K = 1, 10, 100 com P e C
// fixos, e imprime uma tabela comparativa — é o que o enunciado pede em
// "Coleta de dados variando o tamanho do buffer K".
func rodarExperimento(p, c, itens, timeoutMs, amostragemMs int) {
	ks := []int{1, 10, 100}
	impls := []string{"channel", "semaforo"}

	fmt.Printf("=== Experimento: P=%d C=%d itens/produtor=%d, variando K ===\n\n", p, c, itens)
	fmt.Printf("%-10s %-6s %12s %10s %16s %6s   %s\n",
		"impl", "K", "throughput/s", "duração", "ocupação média", "ok?", "itens por consumidor")

	for _, impl := range impls {
		for _, k := range ks {
			cfg := Config{
				Impl:              impl,
				P:                 p,
				C:                 c,
				K:                 k,
				ItensPorProdutor:  itens,
				TimeoutConsumidor: time.Duration(timeoutMs) * time.Millisecond,
				AmostragemMs:      amostragemMs,
			}
			r := rodar(cfg)
			ok := r.Produzido == r.Consumido
			porConsumidor := ""
			for i, v := range r.ItensPorConsumo {
				if i > 0 {
					porConsumidor += " / "
				}
				porConsumidor += fmt.Sprintf("C%d=%d", i, v)
			}
			fmt.Printf("%-10s %-6d %12.1f %10s %16.2f %6v   %s\n",
				impl, k, r.Throughput(), r.Duracao.Round(time.Millisecond), r.OcupacaoMedia, ok, porConsumidor)
			if !ok {
				fmt.Fprintf(os.Stderr, "  ERRO DE INVARIANTE em impl=%s k=%d: produzido=%d consumido=%d\n",
					impl, k, r.Produzido, r.Consumido)
			}
		}
	}
}
