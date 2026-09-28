# T1 — Problemas Clássicos de Concorrência (FPPD)

Integrantes: Tainara Chaves e Guilherme Chaves de Assis

Dois programas Go independentes, cada um com o seu próprio `go.mod`:

| Pasta | Problema |
| --- | --- |
| [`filosofos/`](filosofos/) | Jantar dos Filósofos: versão base (deadlock), hierarquia de recursos e garçom |
| [`produtor-consumidor/`](produtor-consumidor/) | Buffer limitado com channel e com semáforos + mutex |

## Requisitos

- Go 1.24 ou mais recente (versão declarada no `go.mod`)
- Para `go run -race` no Windows: um compilador C (gcc/MinGW-w64) e `CGO_ENABLED=1`

## Como rodar

Sempre de dentro da pasta de cada programa, nunca da raiz:

```bash
cd filosofos
go run . -n 5 -r 15 -estrategia base
go run . -n 5 -r 30 -estrategia hierarquia
go run . -n 5 -r 30 -estrategia garcom

cd ../produtor-consumidor
go run . -impl channel  -p 3 -c 3 -k 10 -itens 200
go run . -impl semaforo -p 3 -c 3 -k 10 -itens 200
go run . -experimento -p 3 -c 3 -itens 150
```

Troque `go run` por `go run -race` para verificar a ausência de data race.
Cada pasta tem um README com as flags e as decisões de projeto.