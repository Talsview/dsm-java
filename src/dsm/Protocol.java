package dsm;

/**
 * Protocolo de mensagens (texto, uma mensagem por linha, campos separados por espaço).
 *
 * Nó -> Coordenador
 *   HELLO <idNo>
 *   READ <var>
 *   WRITE <var> <valor>
 *   LOCK <nome> | UNLOCK <nome>
 *   ACK <numeroDaInvalidacao>
 *   BYE
 *
 * Coordenador -> Nó
 *   WELCOME <idNo>
 *   VALUE <var> <versao> <valor>
 *   WROTE <var> <versao> <valor>
 *   INVALIDATE <var> <numeroDaInvalidacao>
 *   LOCKED <nome> | UNLOCKED <nome>
 *   ERROR <mensagem>
 */
public final class Protocol {
    public static final int DEFAULT_PORT = 5000;

    // Nó -> Coordenador
    public static final String HELLO = "HELLO";
    public static final String READ = "READ";
    public static final String WRITE = "WRITE";
    public static final String LOCK = "LOCK";
    public static final String UNLOCK = "UNLOCK";
    public static final String ACK = "ACK";
    public static final String BYE = "BYE";

    // Coordenador -> Nó
    public static final String WELCOME = "WELCOME";
    public static final String VALUE = "VALUE";
    public static final String WROTE = "WROTE";
    public static final String INVALIDATE = "INVALIDATE";
    public static final String LOCKED = "LOCKED";
    public static final String UNLOCKED = "UNLOCKED";
    public static final String ERROR = "ERROR";

    private Protocol() { }

    /** Nomes de variáveis/travas não podem ter espaços (o espaço separa os campos). */
    public static void validateName(String name) {
        if (name == null || !name.matches("[A-Za-z0-9_.:-]+")) {
            throw new IllegalArgumentException("Nome inválido (use letras, números, _ . : -): " + name);
        }
    }
}
