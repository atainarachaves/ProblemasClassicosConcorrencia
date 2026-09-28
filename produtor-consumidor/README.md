# Produtor/Consumidor com buffer limitado

Duas implementações do mesmo buffer de capacidade **K**:

- **`channel`** (`buffer_channel.go`): `make(chan Item, K)`, usando a
  semântica de bloqueio nativa do Go — sem mutex, sem semáforo explícito.
- **`semaforo`** (`buffer_semaforo.go`): array circular de tamanho K + dois
  semáforos de contagem (`notFull`/`notEmpty`, implementados como
  `chan struct{}` usados só como contador) + `sync.Mutex` protegendo as
  posições de leitura/escrita do array.

## Como rodar

```bash
go run . -impl channel  -p 3 -c 3 -k 10 -itens 300
go run . -impl semaforo -p 3 -c 3 -k 10 -itens 300

# variando K = 1, 10, 100 (dados para o relatório)
go run . -experimento -p 3 -c 3 -itens 150
```

Flags principais: `-p` (produtores), `-c` (consumidores), `-k` (capacidade
do buffer), `-itens` (itens por produtor), `-timeout` (ms do timeout do
consumidor-sentinela), `-amostragem` (ms entre amostras de ocupação).

Checar corretude (nenhuma das duas deve reportar data race):

```bash
go run -race . -impl channel  -p 3 -c 3 -k 10 -itens 200
go run -race . -impl semaforo -p 3 -c 3 -k 10 -itens 200
```

## Por que o mutex é necessário mesmo com semáforos

Os semáforos `notFull`/`notEmpty` só **contam** vagas e itens — eles não
impedem que, com múltiplos produtores, dois deles calculem a mesma posição
`tail` do array e escrevam um sobre o outro, ou que dois consumidores leiam
o mesmo item em `head`. Por isso a região crítica (leitura/escrita do array
e atualização de `head`/`tail`) é protegida por `sync.Mutex`. É esse
detalhe que a implementação com channel dispensa: o channel já serializa
internamente quem escreve e quem lê.

## Encerramento ordenado

- **channel**: uma goroutine separada espera todos os produtores
  terminarem (`wgProdutores.Wait()`) e só então fecha o channel
  (`close(buffer)`) — nunca o lado consumidor fecha. Os consumidores usam
  `v, ok := <-buffer` (ou `range`) para detectar o fechamento e drenar o
  que sobrou antes de sair.
- **semaforo**: não existe `close` para essa estrutura manual. Em vez
  disso, depois que os produtores terminam, o sistema insere exatamente
  **C "poison pills"** (uma por consumidor) na mesma fila. Como a fila é
  FIFO, elas só chegam aos consumidores depois de todo item real — nenhum
  item de trabalho é perdido, e cada consumidor termina ao consumir
  exatamente um poison pill.

Em ambas as versões, ao final é verificado `produzido == consumido`
(o programa sai com código de erro se a igualdade falhar).

## Timeout com `select`

O consumidor de `id == 0` ("sentinela"), em ambas as implementações, usa
`select` com `time.After(timeout)` em vez de receber bloqueado — se o
buffer ficar vazio por mais que `timeout`, ele registra um "idle" e
continua tentando, em vez de ficar parado sem diagnóstico. Isso é visível
rodando com poucos produtores e vários consumidores:

```bash
go run . -impl channel -p 1 -c 4 -k 5 -itens 50 -timeout 2
```

## Métricas coletadas

Por consumidor: itens processados. Agregado: throughput (itens/s),
ocupação média e máxima do buffer (amostrada periodicamente por uma
goroutine de monitoramento, sempre lendo um contador atômico — nunca uma
variável comum compartilhada), e quantas vezes o consumidor-sentinela
disparou o timeout.

O modo `-experimento` roda `channel` e `semaforo` para K = 1, 10, 100 com
P e C fixos e imprime uma tabela comparativa — ponto de partida para a
análise "efeito de K" pedida no relatório.
