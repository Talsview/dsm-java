package dsm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Nó interativo (terminal) para a demonstração ao vivo.
 * Uso: java -cp out dsm.NodeMain <idDoNo> [host] [porta]
 */
public class NodeMain {

    static final String AJUDA =
            "Comandos:\n"
          + "  read <var>                 lê a variável compartilhada\n"
          + "  write <var> <valor>        escreve na variável compartilhada\n"
          + "  inc <var> [pausa_ms]       incrementa COM trava (exclusão mútua)\n"
          + "  incunsafe <var> [pausa_ms] incrementa SEM trava (mostra perda de atualização; pausa padrão 2000)\n"
          + "  lock <nome> | unlock <nome> trava/destrava manualmente\n"
          + "  cache                      mostra as cópias locais deste nó\n"
          + "  stats                      mostra contadores (acertos, falhas, invalidações, mensagens)\n"
          + "  help | quit";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: java -cp out dsm.NodeMain <idDoNo> [host] [porta]");
            return;
        }
        String id = args[0];
        String host = args.length > 1 ? args[1] : "localhost";
        int port = args.length > 2 ? Integer.parseInt(args[2]) : Protocol.DEFAULT_PORT;

        try (DsmNode node = new DsmNode(id, host, port)) {
            node.setVerbose(true);
            System.out.println("Nó " + id + " conectado a " + host + ":" + port + ". Digite 'help'.");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while (true) {
                System.out.print(id + "> ");
                System.out.flush();
                if ((line = in.readLine()) == null) break;
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] p = line.split("\\s+", 3);
                try {
                    switch (p[0].toLowerCase()) {
                        case "read":
                            System.out.println(p[1] + " = '" + node.read(p[1]) + "'");
                            break;
                        case "write":
                            node.write(p[1], p.length > 2 ? p[2] : "");
                            System.out.println("ok: " + p[1] + " atualizado");
                            break;
                        case "inc":
                            increment(node, p[1], p.length > 2 ? Long.parseLong(p[2]) : 0, true);
                            break;
                        case "incunsafe":
                            increment(node, p[1], p.length > 2 ? Long.parseLong(p[2]) : 2000, false);
                            break;
                        case "lock":
                            node.lock(p[1]);
                            System.out.println("trava " + p[1] + " obtida");
                            break;
                        case "unlock":
                            node.unlock(p[1]);
                            System.out.println("trava " + p[1] + " liberada");
                            break;
                        case "cache":
                            System.out.println(node.cacheSnapshot().isEmpty() ? "(cache vazio)" : node.cacheSnapshot());
                            break;
                        case "stats":
                            System.out.println(node.stats());
                            break;
                        case "help":
                            System.out.println(AJUDA);
                            break;
                        case "quit":
                        case "exit":
                            return;
                        default:
                            System.out.println("Comando desconhecido. Digite 'help'.");
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("Faltou argumento. Digite 'help'.");
                } catch (IllegalArgumentException | IOException e) {
                    System.out.println("Erro: " + e.getMessage());
                }
            }
        }
    }

    /** Lê, soma 1 e escreve. Com trava, a sequência é atômica entre os nós. */
    static void increment(DsmNode node, String var, long pauseMs, boolean useLock) throws IOException, InterruptedException {
        String lockName = "lock:" + var;
        if (useLock) node.lock(lockName);
        try {
            String raw = node.read(var);
            long atual = raw.isEmpty() ? 0 : Long.parseLong(raw.trim());
            System.out.println("   leu " + var + " = " + atual + (pauseMs > 0 ? " ... (pausa " + pauseMs + " ms)" : ""));
            if (pauseMs > 0) Thread.sleep(pauseMs);
            node.write(var, String.valueOf(atual + 1));
            System.out.println("   escreveu " + var + " = " + (atual + 1) + (useLock ? "  [com trava]" : "  [SEM trava]"));
        } finally {
            if (useLock) node.unlock(lockName);
        }
    }
}
