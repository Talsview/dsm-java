package dsm;

import java.io.IOException;

/**
 * Abstração de memória compartilhada distribuída (a "visão lógica").
 *
 * A aplicação só enxerga variáveis compartilhadas (read/write) e travas (lock/unlock).
 * Onde o dado realmente está, quantas cópias existem e como elas são mantidas
 * coerentes é responsabilidade da camada DSM (classe DsmNode + DsmServer).
 */
public interface SharedMemory {

    /** Lê o valor de uma variável compartilhada (pode vir da cópia local ou da rede). */
    String read(String var) throws IOException;

    /** Escreve um valor; ao retornar, nenhuma outra cópia antiga da variável permanece válida. */
    void write(String var, String value) throws IOException;

    /** Adquire a trava nomeada (exclusão mútua entre processos). Bloqueia se estiver ocupada. */
    void lock(String name) throws IOException;

    /** Libera a trava nomeada. */
    void unlock(String name) throws IOException;
}
