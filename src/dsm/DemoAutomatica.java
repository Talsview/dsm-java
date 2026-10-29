package dsm;

import java.util.ArrayList;
import java.util.List;

/**
 * Demonstração automática (sem precisar abrir vários terminais).
 * Sobe um coordenador e 3 nós em threads e mostra:
 *   1) leitura com cópia local e invalidação após escrita de outro nó;
 *   2) contador com trava (resultado correto);
 *   3) contador sem trava (perda de atualizações).
 * Uso: java -cp out dsm.DemoAutomatica
 */
public class DemoAutomatica {

    public static void main(String[] args) throws Exception {
        DsmServer server = new DsmServer(0, false);
        server.startAsync();
        int port = server.getPort();

        DsmNode a = new DsmNode("A", "localhost", port);
        DsmNode b = new DsmNode("B", "localhost", port);
        DsmNode c = new DsmNode("C", "localhost", port);

        System.out.println("=== 1) Leitura, cópia local e invalidação ===");
        a.write("x", "5");
        System.out.println("A escreveu x=5.");
        System.out.println("B lê x -> " + b.read("x") + "   (falha de cache: buscou no coordenador)");
        System.out.println("B lê x -> " + b.read("x") + "   (acerto: veio da cópia local de B)");
        System.out.println("Cópias de B antes: " + b.cacheSnapshot());
        a.write("x", "8");
        System.out.println("A escreveu x=8 (o coordenador invalidou a cópia de B).");
        System.out.println("Cópias de B depois: " + b.cacheSnapshot());
        System.out.println("B lê x -> " + b.read("x") + "   (buscou o valor novo)");
        System.out.println("B: " + b.stats());

        System.out.println("\n=== 2) Contador COM trava (3 nós x 30 incrementos) ===");
        a.write("contador", "0");
        int seguro = executar(new DsmNode[]{a, b, c}, 30, true);
        System.out.println("Esperado: 90 | Obtido: " + seguro + (seguro == 90 ? "  -> correto" : "  -> ERRO"));

        System.out.println("\n=== 3) Contador SEM trava (3 nós x 30 incrementos) ===");
        a.write("contador", "0");
        int inseguro = executar(new DsmNode[]{a, b, c}, 30, false);
        System.out.println("Esperado: 90 | Obtido: " + inseguro
                + (inseguro < 90 ? "  -> atualizações perdidas (falta exclusão mútua)" : "  -> sem perdas desta vez (execução sorteada)"));
        System.out.println("(Consistência da cópia local está garantida nos dois casos; o que falta no 3 é exclusão mútua.)");

        a.close(); b.close(); c.close();
        server.stop();
    }

    private static int executar(DsmNode[] nodes, int vezes, boolean comTrava) throws Exception {
        List<Thread> ts = new ArrayList<>();
        for (DsmNode n : nodes) {
            Thread t = new Thread(() -> {
                try {
                    for (int i = 0; i < vezes; i++) {
                        if (comTrava) n.lock("lock:contador");
                        try {
                            long v = Long.parseLong(n.read("contador"));
                            if (!comTrava) Thread.sleep(2);   // alarga a janela entre ler e escrever
                            n.write("contador", String.valueOf(v + 1));
                        } finally {
                            if (comTrava) n.unlock("lock:contador");
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            ts.add(t);
            t.start();
        }
        for (Thread t : ts) t.join();
        return Integer.parseInt(nodes[0].read("contador"));
    }
}
