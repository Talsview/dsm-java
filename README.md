# DSM em Java — Memória Compartilhada Distribuída

Projeto da atividade **Implementação e Apresentação de Modelos de Programação Distribuída** (tema: DSM).
Java puro (sockets + threads), sem bibliotecas externas.

## O que é
Vários processos Java (nós) usam uma **memória compartilhada lógica** por meio de `read(var)` e `write(var, valor)`.
Cada nó mantém **cópias locais**; um **coordenador** central mantém a versão oficial e aplica o protocolo
**write-invalidate** para manter as cópias coerentes. Há ainda **travas** (exclusão mútua) para operações como incrementar um contador.

```
 Nó A (cópia local) ─┐                          ┌─ Nó B (cópia local)
                     ├── rede (TCP) ── Coordenador (valores, copyset, travas)
 Nó C (cópia local) ─┘
```

## Requisitos
JDK 17 ou superior recomendado (testado com JDK 21; o código usa apenas recursos básicos da linguagem).

## Compilar e executar
```
scripts/compilar.sh        (Windows: scripts\compilar.bat)
scripts/servidor.sh        # terminal 1 — porta 5000
scripts/no.sh A            # terminal 2
scripts/no.sh B            # terminal 3
scripts/demo.sh            # alternativa: demonstração automática
```
Sem os scripts: `javac -d out src/dsm/*.java` e `java -cp out dsm.ServerMain` / `java -cp out dsm.NodeMain A`.

## Comandos do nó
`read <var>` · `write <var> <valor>` · `inc <var>` (com trava) · `incunsafe <var> [ms]` (sem trava) ·
`lock/unlock <nome>` · `cache` · `stats` · `help` · `quit`

## Arquivos
| Arquivo | Função |
|---|---|
| `SharedMemory.java` | Interface da abstração (read/write/lock/unlock) |
| `Protocol.java` | Formato das mensagens e validação de nomes |
| `DsmServer.java` / `ServerMain.java` | Coordenador: valores, versões, copyset, invalidação |
| `LockManager.java` | Travas nomeadas |
| `DsmNode.java` | Nó: cópia local, comunicação, tratamento de INVALIDATE |
| `NodeMain.java` | Terminal interativo do nó |
| `DemoAutomatica.java` | Demonstração e verificação automática |

## Consistência e limitações
- Consistência **forte por variável**: a escrita só termina depois que todas as outras cópias foram invalidadas e confirmadas.
- **Não** há ordenação entre variáveis diferentes nem transações.
- O coordenador é **central** (único ponto de falha) e não há replicação nem persistência.
- Não é uma DSM completa (sem páginas de memória virtual nem tolerância a falhas); é uma DSM **baseada em variáveis/objetos** com coordenador.
