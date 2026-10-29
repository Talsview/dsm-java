# DSM em Java — Memória Compartilhada Distribuída

> **Guia técnico, didático e prático** da implementação de *Distributed Shared Memory* (DSM) com Java, sockets TCP, threads, cache local, invalidação de cópias e exclusão mútua.

**Projeto:** Implementação e Apresentação de Modelos de Programação Distribuída  
**Tema:** Memória Compartilhada Distribuída (DSM)  
**Tecnologia:** Java puro — sem dependências externas  
**Ambiente sugerido:** JDK 17+ (JDK 21 recomendado), VS Code e três terminais  
**Porta padrão:** TCP `5000`

---

## Sumário

1. [Objetivo e conceito](#1-objetivo-e-conceito)
2. [Arquitetura do sistema](#2-arquitetura-do-sistema)
3. [Estrutura do projeto e responsabilidades](#3-estrutura-do-projeto-e-responsabilidades)
4. [Requisitos, compilação e execução](#4-requisitos-compilação-e-execução)
5. [Como a conexão TCP funciona](#5-como-a-conexão-tcp-funciona)
6. [Leitura, cache e falhas de cache](#6-leitura-cache-e-falhas-de-cache)
7. [Escrita, versões e invalidação](#7-escrita-versões-e-invalidação)
8. [Consistência x concorrência](#8-consistência-x-concorrência)
9. [Incremento sem trava](#9-incremento-sem-trava)
10. [Incremento com trava](#10-incremento-com-trava)
11. [Protocolo de mensagens](#11-protocolo-de-mensagens)
12. [Threads e estruturas de dados](#12-threads-e-estruturas-de-dados)
13. [Comandos do terminal](#13-comandos-do-terminal)
14. [Roteiro de demonstração manual](#14-roteiro-de-demonstração-manual)
15. [Demonstração automática](#15-demonstração-automática)
16. [Estatísticas e interpretação dos resultados](#16-estatísticas-e-interpretação-dos-resultados)
17. [Limitações reais e pontos de atenção](#17-limitações-reais-e-pontos-de-atenção)
18. [Possíveis problemas e soluções](#18-possíveis-problemas-e-soluções)
19. [Perguntas frequentes para a apresentação](#19-perguntas-frequentes-para-a-apresentação)
20. [Resumo: funcionalidade → código](#20-resumo-funcionalidade--código)

---

## 1. Objetivo e conceito

Um programa Java normalmente acessa apenas os dados de seu próprio processo. A RAM de um computador também não é automaticamente acessível por outro computador. A **DSM** fornece uma *abstração* em que processos separados conseguem trabalhar com dados compartilhados, enquanto um mecanismo de software realiza a comunicação e a coordenação necessárias.

Neste projeto, a memória compartilhada é **lógica e baseada em variáveis nomeadas**, como `x` ou `contador`. Não há uma única RAM física compartilhada nem compartilhamento transparente de endereços de memória do Java.

**Exemplo:** o nó A executa `write x 20`; o nó B executa `read x` e, após a atualização/invalidação apropriada, obtém `20`.

Os objetivos da implementação são:

- oferecer operações simples de `read` e `write` para variáveis compartilhadas;
- manter cópias locais (*cache*) para reduzir leituras de rede;
- invalidar cópias antigas quando alguém escreve;
- coordenar operações críticas por meio de `lock` e `unlock`;
- demonstrar experimentalmente a diferença entre *consistência de cópias* e *exclusão mútua*.

## 2. Arquitetura do sistema

```text
┌─────────────────────┐            ┌────────────────────────────┐
│ Nó A (processo JVM) │            │ Coordenador (processo JVM)│
│  NodeMain           │            │  ServerMain                │
│  DsmNode            │◄── TCP ───►│  DsmServer                 │
│  cache local        │            │  memory: valor + versão    │
└─────────────────────┘            │  copyset: nós com cópia    │
                                   │  LockManager: travas       │
┌─────────────────────┐            └────────────────────────────┘
│ Nó B (processo JVM) │                       ▲
│  NodeMain           │                       │ TCP
│  DsmNode            │◄──────────────────────┘
│  cache local        │
└─────────────────────┘
```

**Coordenador:** guarda o estado oficial das variáveis, controla versões, acompanha quem tem cópia (`copyset`) e gerencia travas.  
**Nós:** mantêm cópias locais, executam comandos e conversam com o servidor via TCP.  
**Sockets:** transportam mensagens de texto; são processos independentes mesmo quando executados no mesmo computador.

A arquitetura é **centralizada**. Se o coordenador parar, o serviço de coordenação fica indisponível.

## 3. Estrutura do projeto e responsabilidades

```text
dsm-java/
├── README.md
├── .gitignore
├── src/
│   └── dsm/
│       ├── SharedMemory.java
│       ├── Protocol.java
│       ├── DsmServer.java
│       ├── ServerMain.java
│       ├── DsmNode.java
│       ├── NodeMain.java
│       ├── LockManager.java
│       └── DemoAutomatica.java
└── scripts/
    ├── compilar.bat  compilar.sh
    ├── servidor.bat  servidor.sh
    ├── no.bat        no.sh
    └── demo.bat      demo.sh
```

| Arquivo | O que faz | Quando entra em ação |
|---|---|---|
| `SharedMemory.java` | Interface da API lógica | Define `read`, `write`, `lock`, `unlock` |
| `Protocol.java` | Constantes do protocolo e validação | Em todas as mensagens entre nó e servidor |
| `DsmServer.java` | Coordenador das variáveis e clientes | Ao conectar, ler, escrever, invalidar e travar |
| `ServerMain.java` | Ponto de entrada do servidor | Ao executar `servidor.bat` |
| `DsmNode.java` | Cliente DSM, cache e receptor TCP | Ao executar comandos em qualquer nó |
| `NodeMain.java` | Interface de comandos do terminal | Em `read`, `write`, `inc`, `cache`, `stats` etc. |
| `LockManager.java` | Gerencia propriedade de travas nomeadas | Em `lock` e `unlock`, inclusive nos incrementos seguros |
| `DemoAutomatica.java` | Testa os conceitos automaticamente | Ao executar `demo.bat` |

### 3.1 Interface `SharedMemory.java`

Esta é a abstração vista pela aplicação:

```java
public interface SharedMemory {
    String read(String var) throws IOException;
    void write(String var, String value) throws IOException;
    void lock(String name) throws IOException;
    void unlock(String name) throws IOException;
}
```

O código que usa a interface não precisa enviar manualmente mensagens de rede: a implementação `DsmNode` faz isso. **Atenção:** a interface abstrata não significa que o acesso seja literalmente ao endereço de RAM de outra JVM.

## 4. Requisitos, compilação e execução

### Requisitos

- JDK 17 ou superior (recomendado JDK 21);
- comandos `java` e `javac` disponíveis no terminal;
- Windows PowerShell, CMD ou terminal Linux/macOS;
- TCP local disponível na porta `5000` para a execução manual padrão.

### Windows / PowerShell — passo a passo

Extraia o ZIP e abra a pasta **`dsm-java`** no VS Code. No terminal, verifique:

```powershell
java -version
javac -version
```

**Compile** na raiz do projeto:

```powershell
.\scripts\compilar.bat
```

O script executa, essencialmente, `javac -encoding UTF-8 -d out src\dsm\*.java`, gerando os arquivos `.class` na pasta `out`.

Abra três terminais independentes **nesta ordem**:

**Terminal 1 — coordenador:**

```powershell
.\scripts\servidor.bat
```

**Terminal 2 — nó A:**

```powershell
.\scripts\no.bat A
```

**Terminal 3 — nó B:**

```powershell
.\scripts\no.bat B
```

Mensagens típicas:

```text
[COORDENADOR] Coordenador DSM ouvindo na porta 5000
[COORDENADOR] nó A conectado
[COORDENADOR] nó B conectado
```

Os terminais dos nós exibem os prompts `A>` e `B>`.

### Linux/macOS

```bash
bash scripts/compilar.sh
bash scripts/servidor.sh   # terminal 1
bash scripts/no.sh A       # terminal 2
bash scripts/no.sh B       # terminal 3
```

### Sem scripts

```powershell
javac -encoding UTF-8 -d out src/dsm/*.java
java -cp out dsm.ServerMain
java -cp out dsm.NodeMain A
java -cp out dsm.NodeMain B
```

No Windows PowerShell, se o curinga `*.java` não funcionar no comando `javac` digitado manualmente, use o script `.bat`, que executa via CMD.

O `NodeMain` aceita `ID`, `host` e `porta` como argumentos posicionais; o padrão é `localhost:5000`. Por exemplo:

```powershell
java -cp out dsm.NodeMain B localhost 5000
```

Para usar máquinas diferentes, execute o coordenador na máquina servidora e informe o IP/hostname dessa máquina nos nós. Configure firewall e acesso à porta somente em uma rede confiável: **o projeto não tem autenticação nem criptografia TLS**.

## 5. Como a conexão TCP funciona

### Arquivos: `ServerMain.java`, `DsmServer.java`, `DsmNode.java` e `Protocol.java`

**1.** `ServerMain.main()` cria um `DsmServer` e chama `start()`.

**2.** `DsmServer.bind()` abre um `ServerSocket`:

```java
serverSocket = new ServerSocket(requestedPort);
```

**3.** `DsmServer.acceptLoop()` aceita conexões e cria uma thread `ClientHandler` para cada conexão:

```java
Socket s = serverSocket.accept();
Thread t = new Thread(new ClientHandler(s), "dsm-client");
t.setDaemon(true);
t.start();
```

**4.** `DsmNode` abre `new Socket(host, port)` e envia `HELLO A` ou `HELLO B`.

**5.** `ClientHandler.run()` registra o ID em `clients` (`ConcurrentHashMap`) e responde `WELCOME A` / `WELCOME B`. IDs duplicados são recusados.

**6.** O `DsmNode` inicia uma thread `readerLoop` para escutar respostas e avisos assíncronos. Assim ele consegue receber `INVALIDATE` mesmo enquanto a aplicação aguarda uma resposta.

**Exemplo de tráfego:**

```text
Nó A  ───── HELLO A ─────► Servidor
Nó A  ◄──── WELCOME A ──── Servidor
```

## 6. Leitura, cache e falhas de cache

### Arquivos: `NodeMain.java` → `DsmNode.read()` → `DsmServer.doRead()`

Digite no nó A `write x 10` e no nó B `read x`.

**Etapa 1 — terminal:** `NodeMain.main()` reconhece `case "read"` e chama `node.read(p[1])`.

**Etapa 2 — procura no cache:** `DsmNode.read()` consulta `cache`, um `ConcurrentHashMap<String, CacheEntry>`:

```java
CacheEntry c = cache.get(var);
if (c != null) {
    hits.incrementAndGet();
    return c.value;
}
misses.incrementAndGet();
send(Protocol.READ + " " + var);
String[] r = await();
return r.length > 3 ? r[3] : "";
```

- **Cache hit / acerto:** existe cópia válida; a leitura retorna sem solicitar o dado pela rede.
- **Cache miss / falha:** não existe cópia; o nó manda `READ x` e espera uma resposta.

**Etapa 3 — servidor:** `DsmServer.ClientHandler.handle()` recebe `READ` e chama `doRead()`:

```java
Entry e = entry(var);
synchronized (e) {
    e.copyset.add(h.id);
    h.send(Protocol.VALUE + " " + var + " " + e.version + " " + e.value);
}
```

O servidor registra B no conjunto dos nós com cópia e envia valor e versão (`VALUE x 1 10`).

**Etapa 4 — recebimento:** `DsmNode.readerLoop()` recebe `VALUE` e cria/atualiza o cache local:

```java
cache.put(p[1], new CacheEntry(Long.parseLong(p[2]),
        p.length > 3 ? p[3] : ""));
responses.offer(p);
```

A resposta também entra na fila `responses` para liberar quem está aguardando em `await()`.

**Teste no nó B:**

```text
B> read x
x = '10'
B> cache
{x=10 (v1)}
B> read x
x = '10'
```

A segunda leitura usa a cópia local (salvo se houver uma invalidação intermediária). Consulte `stats` para conferir os acertos e falhas.

## 7. Escrita, versões e invalidação

### Arquivos: `NodeMain.java` → `DsmNode.write()` → `DsmServer.doWrite()` → `DsmNode.readerLoop()`

Suponha B já tenha lido `x = 10`. Em A, digite `write x 20`.

**1. Nó A envia a operação:**

```java
send(Protocol.WRITE + " " + var + " " + value);
await();
```

Trecho de `DsmNode.write()`. A operação aguarda a resposta do coordenador.

**2. O servidor altera o estado oficial:** cada variável é uma `Entry` contendo `value`, `version` e `copyset`:

```java
private static class Entry {
    String value = "";
    long version = 0;
    final Set<String> copyset = new HashSet<>();
}
```

No método `DsmServer.doWrite()`:

```java
synchronized (e) {
    e.value = value;
    e.version++;
    for (String nodeId : new ArrayList<>(e.copyset)) {
        if (nodeId.equals(h.id)) continue;
        ClientHandler other = clients.get(nodeId);
        if (other != null) other.invalidate(var);
    }
    e.copyset.clear();
    e.copyset.add(h.id);
    h.send(Protocol.WROTE + " " + var + " " + e.version + " " + value);
}
```

O `synchronized(e)` serializa as operações `doRead` e `doWrite` **na mesma variável do lado do coordenador**.

**3. Servidor envia invalidação:** `ClientHandler.invalidate()` manda, por exemplo, `INVALIDATE x 7` (o `7` é o identificador interno dessa invalidação, não a versão de `x`).

**4. Nó B recebe e remove a cópia:**

```java
case Protocol.INVALIDATE:
    cache.remove(p[1]);
    invalidations.incrementAndGet();
    if (verbose)
        System.out.println("   [" + id + "] cópia local de '" + p[1] + "' invalidada");
    send(Protocol.ACK + " " + p[2]);
    break;
```

**5. Confirmação (ACK):** B responde com o identificador da invalidação. O coordenador usa `CountDownLatch` para esperar esse aviso por **até 3 segundos**. Depois envia `WROTE` a A. Se ocorrer timeout, a implementação **registra a falta de resposta, mas não cancela a escrita** — veja as limitações.

**6. Próxima leitura de B:** como a cópia antiga foi removida, `read x` consulta novamente o coordenador e recebe o valor `20` (versão `v2`).

```text
A> write x 20
ok: x atualizado

B>    [B] cópia local de 'x' invalidada
B> cache
(cache vazio)
B> read x
x = '20'
```

**Sequência ilustrativa:**

```text
A                      Servidor                       B
|--- WRITE x 20 --------->|                           |
|                         |--- INVALIDATE x #7 ------>|
|                         |<----------- ACK #7 --------|
|<--- WROTE x 2 20 ------|
|                                                     |
|  (mais tarde)                   <------ READ x ------|
|                         |------ VALUE x 2 20 ------>|
```

Importante: `INVALIDATE` **não envia o valor 20 a B**; apenas obriga B a buscar o valor novo na próxima leitura.

## 8. Consistência x concorrência

São dois problemas diferentes:

- **Consistência de cópias:** impedir que B continue usando a versão 1 de `x` depois que a versão 2 foi confirmada e a invalidação funcionou.
- **Exclusão mútua:** impedir que A e B executem ao mesmo tempo uma operação composta, como **ler → somar 1 → escrever**.

Uma operação `write` não transforma automaticamente um conjunto de operações `read + cálculo + write` em transação atômica. Por isso, o projeto possui dois comandos de incremento: `incunsafe` e `inc`.

## 9. Incremento sem trava

### Arquivo: `NodeMain.java`, método `increment(...)` com `useLock = false`

Prepare a variável:

```text
A> write contador 0
```

Execute quase ao mesmo tempo nos dois nós:

```text
A> incunsafe contador 5000
B> incunsafe contador 5000
```

O número `5000` representa uma pausa de 5000 milissegundos = 5 segundos **entre a leitura e a escrita**, para facilitar a colisão.

O método `NodeMain.increment()` executa essencialmente:

```java
String raw = node.read(var);
long atual = raw.isEmpty() ? 0 : Long.parseLong(raw.trim());
if (pauseMs > 0) Thread.sleep(pauseMs);
node.write(var, String.valueOf(atual + 1));
```

Sem `lock`, uma execução possível é:

| Momento | Nó A | Nó B |
|---|---|---|
| 1 | Lê `0` | Lê `0` |
| 2 | Calcula `1` | Calcula `1` |
| 3 | Escreve `1` | Escreve `1` |
| Resultado | **Contador = 1** | O correto seria **2** |

A segunda escrita substitui o resultado da primeira: ocorreu **atualização perdida**, causada por **condição de corrida**. O resultado depende de sobreposição temporal: em outra execução pode não haver perda.

**No teste manual realizado com os nós A e B**, ambos leram `0`, ambos escreveram `1` e a leitura final foi `1` — evidência do problema.

## 10. Incremento com trava

### Arquivos: `NodeMain.java`, `DsmNode.lock/unlock()`, `DsmServer.ClientHandler.handle()` e `LockManager.java`

Reinicie e execute o incremento seguro em ambos os nós:

```text
A> write contador 0
A> inc contador
B> inc contador
B> read contador
contador = '2'
```

Em `NodeMain.increment()`, se `useLock` é verdadeiro:

```java
String lockName = "lock:" + var;
if (useLock) node.lock(lockName);
try {
    String raw = node.read(var);
    long atual = raw.isEmpty() ? 0 : Long.parseLong(raw.trim());
    if (pauseMs > 0) Thread.sleep(pauseMs);
    node.write(var, String.valueOf(atual + 1));
} finally {
    if (useLock) node.unlock(lockName);
}
```

O bloco `try/finally` é importante: mesmo se a operação dentro do bloco gerar uma exceção, o código **tenta liberar** a trava em `finally`.

`DsmNode.lock()` envia `LOCK lock:contador`; `DsmNode.unlock()` envia `UNLOCK lock:contador`. O servidor roteia essas mensagens ao `LockManager`.

**Trecho central de `LockManager.java`:**

```java
private final Map<String, String> owners = new HashMap<>();

public synchronized void lock(String name, String nodeId)
        throws InterruptedException {
    while (owners.containsKey(name)
            && !owners.get(name).equals(nodeId)) {
        wait();
    }
    owners.put(name, nodeId);
}

public synchronized boolean unlock(String name, String nodeId) {
    if (nodeId.equals(owners.get(name))) {
        owners.remove(name);
        notifyAll();
        return true;
    }
    return false;
}
```

`owners` relaciona uma trava ao nó que a possui. O `synchronized` protege a estrutura no coordenador; `wait()` bloqueia quem tentou obter uma trava ocupada; `notifyAll()` sinaliza aos que aguardam quando ela é liberada. **`synchronized` sozinho, em JVMs diferentes, não seria suficiente**: a exclusão mútua entre nós é coordenada pelas mensagens `LOCK`/`UNLOCK` e pelo `LockManager` central.

**Resultado observado:** A leu `0` e escreveu `1`; B leu `1` e escreveu `2`. As duas atualizações foram preservadas.

## 11. Protocolo de mensagens

### Arquivo: `Protocol.java`

O protocolo usa **texto UTF-8**, uma mensagem por linha, com campos separados por espaços. `Protocol.validateName()` aceita nomes com letras, números, `_`, `.`, `:` ou `-`. Não permite espaços nos nomes.

| Sentido | Mensagem | Exemplo | Significado |
|---|---|---|---|
| Nó → Servidor | `HELLO` | `HELLO A` | Registrar nó |
| Servidor → Nó | `WELCOME` | `WELCOME A` | Confirmar conexão |
| Nó → Servidor | `READ` | `READ x` | Solicitar valor |
| Servidor → Nó | `VALUE` | `VALUE x 1 10` | Entregar valor e versão |
| Nó → Servidor | `WRITE` | `WRITE x 20` | Solicitar alteração |
| Servidor → Nó | `WROTE` | `WROTE x 2 20` | Confirmar escrita |
| Servidor → Nó | `INVALIDATE` | `INVALIDATE x 7` | Descartar cópia de `x` |
| Nó → Servidor | `ACK` | `ACK 7` | Confirmar uma invalidação |
| Nó → Servidor | `LOCK` | `LOCK lock:contador` | Solicitar trava |
| Servidor → Nó | `LOCKED` | `LOCKED lock:contador` | Trava concedida |
| Nó → Servidor | `UNLOCK` | `UNLOCK lock:contador` | Liberar trava |
| Servidor → Nó | `UNLOCKED` | `UNLOCKED lock:contador` | Confirmar liberação |
| Servidor → Nó | `ERROR` | `ERROR ...` | Informar erro |
| Nó → Servidor | `BYE` | `BYE` | Encerrar sessão |

A constante `Protocol.DEFAULT_PORT` é `5000`. Há validação do nome, mas **não** há autenticação de nós, validação completa de formatos e criptografia de conexão. Evite expor esse serviço diretamente à internet.

## 12. Threads e estruturas de dados

### Por que o projeto usa threads?

- `DsmServer.acceptLoop()`: recebe novas conexões e cria uma thread `ClientHandler` por cliente;
- `ClientHandler.run()`: lê mensagens de cada conexão e encaminha requisições a um `ExecutorService`, permitindo que o laço ainda receba `ACK` enquanto outra thread espera a invalidação;
- `DsmNode.readerLoop()`: recebe `VALUE`, `WROTE`, `INVALIDATE` e outras mensagens assíncronas;
- `DemoAutomatica.executar()`: inicia uma thread por nó para simular incrementos simultâneos.

### Principais estruturas

| Estrutura | Local | Para que serve |
|---|---|---|
| `ConcurrentHashMap<String, Entry> memory` | `DsmServer` | Registro de variáveis compartilhadas |
| `Entry.value` e `Entry.version` | `DsmServer` | Valor oficial e versão |
| `Entry.copyset` (`HashSet`) | `DsmServer` | Nós que possuem cópia válida |
| `ConcurrentHashMap<String, ClientHandler> clients` | `DsmServer` | Conexões registradas |
| `ConcurrentHashMap<String, CacheEntry> cache` | `DsmNode` | Cópias locais e versões |
| `LinkedBlockingQueue<String[]> responses` | `DsmNode` | Passagem de respostas da thread leitora ao comando aguardando |
| `CountDownLatch pending` | `ClientHandler` | Esperar cada `ACK` de invalidação |
| `HashMap<String, String> owners` | `LockManager` | Dono de cada trava |
| `AtomicInteger` | `DsmNode` | Contadores de métricas |

`cache` e `memory` são mapas seguros para acesso concorrente, mas isso **não dispensa** a sincronização lógica entre processos. A sincronização por `Entry` e as travas tratam problemas de escopo diferente.

## 13. Comandos do terminal

| Comando | Exemplo | Explicação |
|---|---|---|
| `help` | `help` | Mostra os comandos disponíveis |
| `read <var>` | `read x` | Lê do cache ou do servidor |
| `write <var> <valor>` | `write x 20` | Grava novo valor compartilhado |
| `cache` | `cache` | Mostra as cópias locais válidas |
| `stats` | `stats` | Mostra acertos, falhas, invalidações e mensagens |
| `inc <var> [ms]` | `inc contador` | Incrementa com trava; pausa opcional |
| `incunsafe <var> [ms]` | `incunsafe contador 5000` | Incrementa sem trava; pausa padrão de 2000 ms |
| `lock <nome>` | `lock recurso` | Pede uma trava explicitamente |
| `unlock <nome>` | `unlock recurso` | Libera a trava que o nó possui |
| `quit` / `exit` | `quit` | Encerra o nó |

> `inc` e `incunsafe` são comandos de conveniência escritos em `NodeMain`, e não operações definidas em `SharedMemory`. Ambos usam `read` e `write` por baixo.

## 14. Roteiro de demonstração manual

Este roteiro reproduz o tipo de execução já observado nos terminais A e B.

### Parte 1 — Compartilhar variável e observar cache

1. **A:** `write x 10` → servidor armazena `x = 10` (normalmente versão 1 em um servidor recém-iniciado).
2. **B:** `read x` → imprime `10`; leitura remota por cache miss.
3. **B:** `cache` → mostra `{x=10 (v1)}`.
4. **B:** `read x` → imprime `10`; usa cache se não ocorreu invalidação.
5. **A:** `write x 20` → servidor altera valor e manda `INVALIDATE` a B.
6. **B:** recebe `cópia local de 'x' invalidada`.
7. **B:** `cache` → `(cache vazio)` se não há outras variáveis em cache.
8. **B:** `read x` → imprime `20`, recebido novamente do servidor.

**O que dizer:** “B não compartilha a RAM física de A; B mantém sua própria cópia. Quando A altera x, o servidor invalida a cópia antiga de B, que faz nova leitura quando precisa.”

### Parte 2 — Condição de corrida (sem trava)

1. **A:** `write contador 0`.
2. **A e B, quase simultaneamente:** `incunsafe contador 5000`.
3. Observe se ambos imprimem `leu contador = 0` antes de escrever.
4. Depois que terminarem: **B:** `read contador`.
5. Se as operações se sobrepuserem, o resultado pode ser `1`, mostrando uma atualização perdida.

**O que dizer:** “Cada nó incrementou, mas ambos partiram do mesmo zero. A sequência de leitura, soma e escrita não foi protegida por uma trava.”

### Parte 3 — Exclusão mútua (com trava)

1. **A:** `write contador 0`.
2. **A:** `inc contador`.
3. **B:** `inc contador`.
4. **B:** `read contador` → espera-se `2` após os dois incrementos concluídos.

**O que dizer:** “O nó obtém a trava antes de ler e escrever. Outro nó só pode realizar essa sequência após a liberação; nenhuma atualização é perdida.”

### Parte 4 — Métricas

Em B, digite `stats`. Explique os números usando a seção seguinte.

## 15. Demonstração automática

### Arquivo: `DemoAutomatica.java`

Após compilar, execute **sem precisar abrir manualmente o servidor e os nós**:

```powershell
.\scripts\demo.bat
```

Ou:

```bash
bash scripts/demo.sh
```

`DemoAutomatica.main()` cria um `DsmServer(0, false)`: usar a porta `0` faz o sistema operacional escolher uma porta disponível. O programa chama `startAsync()`, abre três conexões `DsmNode` (`A`, `B`, `C`), executa testes e fecha recursos.

**Teste 1 — cache e invalidação:** A escreve `x = 5`; B lê duas vezes; A escreve `x = 8`; B perde a cópia e depois lê `8`.

**Teste 2 — contador com trava:** três nós fazem **30 incrementos cada**, total esperado de **90**. Cada thread usa `n.lock("lock:contador")` antes de `read` e `write`, e `n.unlock(...)` no `finally`.

**Teste 3 — contador sem trava:** três nós fazem 30 incrementos cada, mas agora o programa omite a trava e usa `Thread.sleep(2)` entre ler e escrever para aumentar a chance de colisões. O total pode ser menor que 90; não há número exato garantido para toda execução.

Trecho simplificado de `DemoAutomatica.executar()`:

```java
if (comTrava) n.lock("lock:contador");
try {
    long v = Long.parseLong(n.read("contador"));
    if (!comTrava) Thread.sleep(2);
    n.write("contador", String.valueOf(v + 1));
} finally {
    if (comTrava) n.unlock("lock:contador");
}
```

> A execução automática usa concorrência real entre threads. O agendamento varia conforme CPU, sistema operacional e momento da execução; o resultado sem trava não deve ser apresentado como um valor fixo.

## 16. Estatísticas e interpretação dos resultados

### Arquivo: `DsmNode.java`

O método `stats()` imprime:

```text
acertos de cache=N, falhas=M, invalidações recebidas=K, mensagens enviadas=P
```

- **Acertos de cache:** quantas leituras retornaram diretamente da cópia local (`hits.incrementAndGet()`).
- **Falhas de cache:** quantas leituras precisaram solicitar valor ao servidor (`misses.incrementAndGet()`).
- **Invalidações recebidas:** quantos avisos `INVALIDATE` o nó processou (`invalidations.incrementAndGet()`).
- **Mensagens enviadas:** quantidade de chamadas ao método `send()`, inclusive protocolo de conexão, leituras, escritas, travas e ACKs (`messagesSent.incrementAndGet()`).

**Exemplo de interpretação:** se B faz `read x` pela primeira vez, espera-se um *miss*; se lê `x` novamente sem alterações, espera-se um *hit*; se A altera `x`, B recebe uma invalidação; na leitura seguinte, B volta a registrar um *miss*.

## 17. Limitações reais e pontos de atenção

Este é um **protótipo didático** de DSM centralizada, não uma plataforma de memória distribuída pronta para produção.

1. **Ponto único de falha:** há um único `DsmServer`; não existem réplicas ou eleição de novo coordenador.
2. **Sem persistência:** os valores vivem em memória; reiniciar o servidor apaga o estado.
3. **Granularidade por variável:** compartilha nomes/valores, não páginas da memória virtual nem objetos Java arbitrários de forma transparente.
4. **Sem transações entre variáveis:** o `synchronized(e)` coordena uma variável por vez; não garante atomicidade conjunta entre `x` e `y`.
5. **Timeout de invalidação:** `ClientHandler.invalidate()` espera `ACK` por até **3 segundos** usando `CountDownLatch.await(3, TimeUnit.SECONDS)`. Se o ACK não chegar, o servidor apenas registra `SEM resposta` e **continua** com o fluxo. Portanto, a alegação de “consistência forte por variável” deve ser entendida **no caminho normal, com invalidação efetivamente confirmada**, não como garantia absoluta sob falhas de comunicação.
6. **Segurança:** não existem autenticação de nós, controle de permissões ou TLS; use somente em ambiente de demonstração/rede controlada.
7. **Mensagens simples:** protocolo textual orientado a linhas, sem tratamento robusto de todos os casos de entrada malformada, falha de rede ou recuperação de operação.
8. **Dependência das travas:** para um incremento composto ser seguro, todas as aplicações que modificam aquele contador devem cooperar usando a mesma trava; `write` isolado não obriga a obtê-la.
9. **Sem medição de desempenho rigorosa:** o projeto mostra contadores de cache e exemplos de concorrência, não benchmarks padronizados de latência, throughput e escalabilidade.

## 18. Possíveis problemas e soluções

| Problema | Causa provável | O que verificar |
|---|---|---|
| `java`/`javac` não reconhecido | JDK ou PATH ausente | Execute `java -version` / `javac -version`; configure o JDK |
| Erro `Connection refused` | Servidor não iniciado ou porta/host incorretos | Inicie `servidor.bat` antes dos nós; confira `localhost:5000` |
| Porta 5000 ocupada | Outro processo já usa a porta | Pare o servidor anterior ou escolha outra porta nos argumentos |
| `id em uso` | Dois nós com o mesmo ID | Use IDs distintos como `A`, `B`, `C` |
| `cache` não mostra x | Ainda não leu/escreveu x, ou houve invalidação | Execute `read x` e consulte o cache novamente |
| Incremento sem trava dá 2 | Não ocorreu colisão naquela tentativa | Inicie ambos quase juntos e aumente a pausa de `incunsafe` |
| Valor desapareceu após reiniciar | Não existe persistência | Refaça `write` após a inicialização |
| Caracteres acentuados aparecem errados | Codificação do terminal | Use `.bat` que configura página UTF-8 ou terminal com UTF-8 |
| Nó não consegue conectar de outro PC | Host, firewall ou rede | Use IP do servidor e confirme acesso TCP na rede confiável |

## 19. Perguntas frequentes para a apresentação

**Isso é realmente memória compartilhada?**  
É **memória compartilhada lógica** por variável, implementada com mensagens entre processos e cópias locais; não é uma RAM física comum.

**Qual a diferença entre `read` e consultar uma variável Java normal?**  
`read` consulta o cache e, se necessário, usa a rede para buscar o valor do coordenador. Uma variável comum pertence ao processo local.

**Por que o servidor guarda `copyset`?**  
Para saber quais nós possuem cópias válidas e quais devem ser avisados após uma nova escrita.

**Para que serve a versão (`v1`, `v2`)?**  
Para identificar a evolução dos valores armazenados e documentar qual versão está no cache. Neste protótipo, a invalidação é feita pela mensagem do servidor, não por comparação periódica de versões.

**O que significa `ACK`?**  
É a confirmação de que o nó recebeu e processou uma invalidação. O servidor tenta esperar essa confirmação antes de responder ao escritor.

**O servidor precisa ser outro computador?**  
Não. Para fins didáticos, três processos Java no mesmo PC são suficientes. A comunicação continua ocorrendo por TCP.

**Por que `incunsafe` pode falhar se cada `write` funciona?**  
Porque `read + somar + write` são passos separados; outro nó pode interferir entre eles.

**O `synchronized` resolve a sincronização distribuída sozinho?**  
Não. Ele protege a execução concorrente *dentro de uma JVM*. Para coordenar nós em JVMs diferentes, o projeto usa `LOCK`, `UNLOCK` e um `LockManager` no coordenador.

**Por que não usar Redis ou um banco de dados?**  
O objetivo da atividade é implementar e demonstrar diretamente os mecanismos de uma DSM: sockets, cópias locais, invalidação e sincronização, sem esconder esses mecanismos numa ferramenta externa.

## 20. Resumo: funcionalidade → código

Use esta tabela quando alguém perguntar **“onde exatamente isso acontece no Java?”**:

| Funcionalidade demonstrada | Arquivo(s) | Método(s) / trecho(s) |
|---|---|---|
| Subir o coordenador | `ServerMain`, `DsmServer` | `main`, `start`, `bind`, `acceptLoop` |
| Conectar nó A e B | `DsmNode`, `DsmServer` | Construtor `DsmNode`, `ClientHandler.run`, `HELLO/WELCOME` |
| Ler `x` | `NodeMain`, `DsmNode`, `DsmServer` | `case "read"`, `read`, `doRead` |
| Acerto de cache | `DsmNode` | `cache.get`, `hits.incrementAndGet` |
| Falha de cache | `DsmNode` | `misses.incrementAndGet`, `send(READ...)`, `await` |
| Guardar cópia e versão | `DsmNode` | `readerLoop`, `cache.put`, `CacheEntry` |
| Escrever `x` | `NodeMain`, `DsmNode`, `DsmServer` | `case "write"`, `write`, `doWrite` |
| Atualizar versão oficial | `DsmServer` | `Entry`, `e.value = value`, `e.version++` |
| Localizar nós com cópia | `DsmServer` | `Entry.copyset`, `doRead`, `doWrite` |
| Invalidar cache de B | `DsmServer`, `DsmNode` | `ClientHandler.invalidate`, `readerLoop / INVALIDATE` |
| Confirmar invalidação | `DsmNode`, `DsmServer` | `send(ACK...)`, `pending`, `CountDownLatch` |
| Mostrar cache | `NodeMain`, `DsmNode` | `case "cache"`, `cacheSnapshot` |
| Mostrar estatísticas | `NodeMain`, `DsmNode` | `case "stats"`, `stats` |
| Incremento sem trava | `NodeMain` | `case "incunsafe"`, `increment(..., false)` |
| Incremento seguro | `NodeMain`, `DsmNode`, `LockManager` | `case "inc"`, `increment(..., true)`, `lock`, `unlock` |
| Liberar travas após desconexão | `DsmServer`, `LockManager` | `disconnect`, `releaseAll` |
| Testes com 3 nós | `DemoAutomatica` | `main`, `executar` |

### Conclusão

O programa comprova o conceito central de DSM: **processos com memórias físicas separadas conseguem trabalhar com dados logicamente compartilhados**. Essa abstração é oferecida pelas operações `read/write`, implementada por **TCP + cópias locais + diretório central + invalidação**, e complementada por **travas de exclusão mútua** quando várias operações precisam formar uma seção crítica.

Na apresentação, mostre primeiro o **cache e a invalidação**, depois a **atualização perdida sem trava** e, por fim, o **resultado correto com trava**. Isso conecta, de forma verificável, cada efeito do terminal ao código Java que o produz.
