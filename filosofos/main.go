// Comando: filosofos
//
// Simula o Jantar dos Filósofos com três estratégias de sincronização:
//
//	base       -> todos pegam o garfo esquerdo primeiro (DEVE gerar deadlock)
//	hierarquia -> ordem global dos garfos (quebra espera circular)
//	garcom     -> semáforo limitando N-1 filósofos à mesa (quebra espera circular)
//
// Garfos são representados por channels `chan struct{}` de capacidade 1
// (um "token" no channel = garfo disponível na mesa).
//
// Uso:
//
//	go run . -n 5 -r 10 -estrategia base
//	go run . -n 5 -r 20 -estrategia hierarquia
//	go run . -n 5 -r 20 -estrategia garcom
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Stats acumula métricas por filósofo. Todos os campos são acessados
// concorrentemente e por isso usam sync/atomic (nunca leitura/escrita crua).
type Stats struct {
	refeicoes []int64 // refeições completadas por filósofo
	esperaNs  []int64 // tempo total (ns) esperando pelos garfos, por filósofo
}

func novoStats(n int) *Stats {
	return &Stats{
		refeicoes: make([]int64, n),
		esperaNs:  make([]int64, n),
	}
}

// pegarGarfo tenta receber o token do garfo. Retorna false se `quit` for
// fechado antes (sinal de que um deadlock foi detectado e o programa está
// encerrando de forma ordenada).
func pegarGarfo(garfo chan struct{}, quit <-chan struct{}) bool {
	select {
	case <-garfo:
		return true
	case <-quit:
		return false
	}
}

func devolverGarfo(garfo chan struct{}) {
	garfo <- struct{}{}
}

func dormirOuSair(d time.Duration, quit <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-quit:
		return false
	}
}

// filosofo executa o ciclo pensar -> pegar garfos -> comer -> devolver garfos,
// repetindo `r` vezes, a menos que `quit` seja fechado antes (deadlock forçado
// ou encerramento).
func filosofo(
	id, n, r int,
	estrategia string,
	garfos []chan struct{},
	sem chan struct{},
	stats *Stats,
	lastProgress *int64,
	quit <-chan struct{},
	start <-chan struct{},
	barreira chan struct{},
	contagemEsquerda *int64,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1000))

	<-start // aguarda todos os filósofos estarem prontos (início sincronizado)

	esquerdo := id
	direito := (id + 1) % n

	for i := 0; i < r; i++ {
		// --- pensar ---
		if !dormirOuSair(time.Duration(10+rng.Intn(20))*time.Millisecond, quit) {
			return
		}

		tQuero := time.Now()

		// --- adquirir garfos conforme a estratégia ---
		var primeiro, segundo chan struct{}

		switch estrategia {
		case "hierarquia":
			// Ordem global: sempre pega o garfo de menor índice primeiro.
			// Isso quebra a espera circular (uma das condições de Coffman).
			if esquerdo < direito {
				primeiro, segundo = garfos[esquerdo], garfos[direito]
			} else {
				primeiro, segundo = garfos[direito], garfos[esquerdo]
			}
		default: // "base" e "garcom" pegam esquerdo -> direito
			primeiro, segundo = garfos[esquerdo], garfos[direito]
		}

		if estrategia == "garcom" {
			// Garçom: só entra na disputa pelos garfos se houver uma vaga
			// entre as N-1 disponíveis. Isso impede que os N filósofos
			// segurem um garfo cada simultaneamente (retenção e espera +
			// espera circular).
			select {
			case sem <- struct{}{}:
			case <-quit:
				return
			}
		}

		if !pegarGarfo(primeiro, quit) {
			if estrategia == "garcom" {
				<-sem
			}
			return
		}

		// Barreira só usada na versão "base", apenas na 1ª iteração: força
		// TODOS os filósofos a segurarem o garfo esquerdo antes que qualquer
		// um tente o direito. Sem isso, a variação natural dos tempos de
		// pensar/comer pode fazer a corrida "por sorte" não travar sempre —
		// e o enunciado exige que a versão base trave em deadlock de forma
		// determinística.
		if estrategia == "base" && i == 0 {
			c := atomic.AddInt64(contagemEsquerda, 1)
			if c == int64(n) {
				close(barreira)
			} else {
				select {
				case <-barreira:
				case <-quit:
					devolverGarfo(primeiro)
					return
				}
			}
		}

		if !pegarGarfo(segundo, quit) {
			devolverGarfo(primeiro)
			if estrategia == "garcom" {
				<-sem
			}
			return
		}

		espera := time.Since(tQuero)
		atomic.AddInt64(&stats.esperaNs[id], espera.Nanoseconds())

		// --- comer ---
		dormirOuSair(time.Duration(10+rng.Intn(20))*time.Millisecond, quit)

		devolverGarfo(segundo)
		devolverGarfo(primeiro)
		if estrategia == "garcom" {
			<-sem
		}

		atomic.AddInt64(&stats.refeicoes[id], 1)
		atomic.StoreInt64(lastProgress, time.Now().UnixNano())
	}
}

func main() {
	n := flag.Int("n", 5, "número de filósofos")
	r := flag.Int("r", 15, "número de iterações (refeições-alvo) por filósofo")
	estrategia := flag.String("estrategia", "base", "estratégia: base | hierarquia | garcom")
	timeoutMs := flag.Int("timeout", 1500, "ms sem progresso para declarar deadlock (watchdog)")
	flag.Parse()

	if *n < 2 {
		fmt.Fprintln(os.Stderr, "n deve ser >= 2")
		os.Exit(1)
	}
	if *estrategia != "base" && *estrategia != "hierarquia" && *estrategia != "garcom" {
		fmt.Fprintln(os.Stderr, "estrategia deve ser: base, hierarquia ou garcom")
		os.Exit(1)
	}

	n_, r_ := *n, *r

	garfos := make([]chan struct{}, n_)
	for i := range garfos {
		garfos[i] = make(chan struct{}, 1)
		garfos[i] <- struct{}{} // garfo começa disponível na mesa
	}

	var sem chan struct{}
	if *estrategia == "garcom" {
		sem = make(chan struct{}, n_-1) // no máximo N-1 filósofos disputando garfos
	}

	stats := novoStats(n_)
	quit := make(chan struct{})
	start := make(chan struct{})
	barreira := make(chan struct{})
	contagemEsquerda := new(int64)
	lastProgress := new(int64)

	var wg sync.WaitGroup
	wg.Add(n_)
	inicio := time.Now()
	atomic.StoreInt64(lastProgress, inicio.UnixNano())

	for i := 0; i < n_; i++ {
		go filosofo(i, n_, r_, *estrategia, garfos, sem, stats, lastProgress, quit, start, barreira, contagemEsquerda, &wg)
	}
	close(start) // início sincronizado: maximiza a chance real de deadlock na versão base

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	deadlockDetectado := false
	timeout := time.Duration(*timeoutMs) * time.Millisecond
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-finished:
			break loop
		case <-ticker.C:
			ultimo := time.Unix(0, atomic.LoadInt64(lastProgress))
			if time.Since(ultimo) > timeout {
				deadlockDetectado = true
				close(quit) // libera todas as goroutines bloqueadas em select
				<-finished  // aguarda encerramento ordenado (nenhuma goroutine vazada)
				break loop
			}
		}
	}

	duracao := time.Since(inicio)

	fmt.Printf("=== Jantar dos Filósofos — estratégia: %s (N=%d, R=%d) ===\n", *estrategia, n_, r_)
	if deadlockDetectado {
		fmt.Println("!! DEADLOCK DETECTADO pelo watchdog (sem progresso por", timeout, ") !!")
		fmt.Println("   Encerramento forçado e ordenado via canal `quit`.")
	} else {
		fmt.Println("Todos os filósofos completaram suas refeições sem deadlock.")
	}
	fmt.Println()

	var totalRef, totalEsperaNs int64
	minRef, maxRef := int64(1<<62), int64(0)
	for i := 0; i < n_; i++ {
		ref := atomic.LoadInt64(&stats.refeicoes[i])
		esp := atomic.LoadInt64(&stats.esperaNs[i])
		totalRef += ref
		totalEsperaNs += esp
		if ref < minRef {
			minRef = ref
		}
		if ref > maxRef {
			maxRef = ref
		}
		mediaEsperaMs := 0.0
		if ref > 0 {
			mediaEsperaMs = float64(esp) / float64(ref) / 1e6
		}
		fmt.Printf("Filósofo %d: %3d refeições | espera média por refeição: %.2f ms\n", i, ref, mediaEsperaMs)
	}

	fmt.Println()
	fmt.Printf("Total de refeições: %d\n", totalRef)
	if totalRef > 0 {
		fmt.Printf("Espera média geral: %.2f ms\n", float64(totalEsperaNs)/float64(totalRef)/1e6)
	}
	fmt.Printf("Fairness (max-min refeições entre filósofos): %d\n", maxRef-minRef)
	fmt.Printf("Tempo total de execução: %s\n", duracao)

	if deadlockDetectado {
		os.Exit(2)
	}
}
