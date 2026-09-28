# Jantar dos Filósofos

Implementação em Go usando **channels** como garfos (`chan struct{}` de
capacidade 1 — um token no channel representa o garfo disponível na mesa).

## Como rodar

```bash
go run . -n 5 -r 15 -estrategia base        # deve travar em deadlock (proposital)
go run . -n 5 -r 30 -estrategia hierarquia  # ordem global dos garfos
go run . -n 5 -r 30 -estrategia garcom      # semáforo N-1 (limita filósofos à mesa)
```

Flags:
- `-n`   número de filósofos (padrão 5)
- `-r`   iterações-alvo por filósofo (padrão 15)
- `-estrategia`  `base` | `hierarquia` | `garcom`
- `-timeout`  ms sem progresso para o watchdog declarar deadlock (padrão 1500)

Checar corretude (nenhum dos três deve reportar data race):

```bash
go run -race . -n 5 -r 15 -estrategia base
go run -race . -n 5 -r 30 -estrategia hierarquia
go run -race . -n 5 -r 30 -estrategia garcom
```

**Windows:** o `-race` precisa de cgo e de um compilador C. Se aparecer
`-race requires cgo`, instale o gcc (por exemplo
`winget install -e --id BrechtSanders.WinLibs.POSIX.UCRT`), abra um terminal
novo e rode `go env -w CGO_ENABLED=1` uma vez.

A versão `base` termina com `exit status 2` de propósito: é o código de
saída que indica que o deadlock foi detectado.

## Como cada versão evita (ou não) o deadlock

- **base**: todos os filósofos pegam o garfo esquerdo primeiro. Uma barreira
  interna força **todos** a segurarem o garfo esquerdo antes que qualquer um
  tente o direito — isso reproduz de forma determinística (não apenas "por
  azar de timing") as quatro condições de Coffman simultaneamente:
  exclusão mútua (cada garfo é de um só filósofo por vez), retenção e espera
  (segura o esquerdo enquanto espera o direito), não preempção (ninguém tira
  o garfo de ninguém) e espera circular (0→1→2→3→4→0). Um **watchdog**
  monitora o progresso global; ao detectar `timeout` sem nenhuma refeição
  concluída, fecha o channel `quit`, que libera todas as goroutines
  bloqueadas em `select` — encerramento ordenado, sem goroutines vazadas.

- **hierarquia**: cada filósofo pega sempre o garfo de **menor índice**
  primeiro. Isso quebra a espera circular: não existe mais um ciclo em que
  cada processo espera o próximo, porque o filósofo N-1 (cujos garfos são
  N-1 e 0) pega o garfo 0 primeiro, na mesma ordem que o filósofo 0.

- **garcom**: um semáforo de contagem (`chan struct{}` com capacidade N-1)
  deve ser adquirido antes de tentar qualquer garfo. Isso impede que os N
  filósofos estejam todos simultaneamente com um garfo na mão (quebra
  retenção-e-espera no nível do sistema), então ao menos um sempre consegue
  os dois garfos e libera o ciclo.

## Métricas coletadas

Por filósofo: refeições completadas e tempo médio de espera pelos garfos.
Agregado: total de refeições, espera média geral e `fairness` (diferença
entre o filósofo que mais e o que menos comeu).