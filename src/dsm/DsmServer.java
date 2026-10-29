package dsm;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordenador da DSM (diretório central).
 *
 * Papel: guarda a versão "oficial" de cada variável compartilhada, sabe quais nós
 * possuem cópia (copyset) e aplica o protocolo WRITE-INVALIDATE:
 *   - leitura: o nó recebe o valor e passa a constar no copyset;
 *   - escrita: o coordenador invalida (e espera confirmação de) todas as outras cópias
 *     antes de responder ao escritor.
 * Por isso, depois que uma escrita termina, nenhuma leitura pode devolver valor antigo
 * (consistência forte por variável). Também gerencia as travas (LockManager).
 *
 * Limitação assumida: o coordenador é um ponto central (e único ponto de falha).
 */
public class DsmServer {

    /** Estado de uma variável compartilhada no coordenador. */
    private static class Entry {
        String value = "";
        long version = 0;
        final Set<String> copyset = new HashSet<>(); // nós que têm cópia local válida
    }

    private final int requestedPort;
    private final Map<String, Entry> memory = new ConcurrentHashMap<>();
    private final Map<String, ClientHandler> clients = new ConcurrentHashMap<>();
    private final LockManager locks = new LockManager();
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "dsm-req");
        t.setDaemon(true);
        return t;
    });
    private final AtomicLong invSeq = new AtomicLong();
    private final boolean verbose;
    private ServerSocket serverSocket;
    private volatile boolean running = true;

    public DsmServer(int port, boolean verbose) {
        this.requestedPort = port;
        this.verbose = verbose;
    }

    private void log(String msg) {
        if (verbose) System.out.println("[COORDENADOR] " + msg);
    }

    public int getPort() { return serverSocket.getLocalPort(); }

    /** Abre a porta e atende em segundo plano (usado pela demo automática). */
    public void startAsync() throws IOException {
        bind();
        Thread t = new Thread(this::acceptLoop, "dsm-accept");
        t.setDaemon(true);
        t.start();
    }

    /** Abre a porta e atende nesta thread (usado por ServerMain). */
    public void start() throws IOException {
        bind();
        acceptLoop();
    }

    private void bind() throws IOException {
        serverSocket = new ServerSocket(requestedPort);
        log("Coordenador DSM ouvindo na porta " + serverSocket.getLocalPort());
    }

    public void stop() {
        running = false;
        try { serverSocket.close(); } catch (IOException ignored) { }
        pool.shutdownNow();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = serverSocket.accept();
                Thread t = new Thread(new ClientHandler(s), "dsm-client");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                if (running) log("erro ao aceitar conexão: " + e.getMessage());
            }
        }
    }

    // ---------------------------------------------------------------- operações DSM

    private Entry entry(String var) {
        return memory.computeIfAbsent(var, k -> new Entry());
    }

    /** Leitura: registra o nó no copyset e devolve o valor (enviado dentro do bloqueio da variável). */
    private void doRead(ClientHandler h, String var) {
        Entry e = entry(var);
        synchronized (e) {
            e.copyset.add(h.id);
            h.send(Protocol.VALUE + " " + var + " " + e.version + " " + e.value);
            log("READ  " + var + " -> " + h.id + " (v" + e.version + ", valor='" + e.value + "')");
        }
    }

    /** Escrita: atualiza, invalida as outras cópias (esperando ACK) e só então responde. */
    private void doWrite(ClientHandler h, String var, String value) {
        Entry e = entry(var);
        synchronized (e) {
            e.value = value;
            e.version++;
            log("WRITE " + var + " = '" + value + "' por " + h.id + " (v" + e.version + ")");
            for (String nodeId : new ArrayList<>(e.copyset)) {
                if (nodeId.equals(h.id)) continue;
                ClientHandler other = clients.get(nodeId);
                if (other != null) other.invalidate(var);
            }
            e.copyset.clear();
            e.copyset.add(h.id);
            h.send(Protocol.WROTE + " " + var + " " + e.version + " " + value);
        }
    }

    // ---------------------------------------------------------------- conexão com cada nó

    private class ClientHandler implements Runnable {
        private final Socket socket;
        private PrintWriter out;
        private volatile boolean closed = false;
        private boolean registered = false;
        private final Map<Long, CountDownLatch> pending = new ConcurrentHashMap<>();
        String id = "?";

        ClientHandler(Socket socket) { this.socket = socket; }

        synchronized void send(String line) {
            if (out != null) out.println(line);
        }

        /** Manda INVALIDATE e espera o ACK do nó (até 3 s). */
        void invalidate(String var) {
            long n = invSeq.incrementAndGet();
            CountDownLatch latch = new CountDownLatch(1);
            pending.put(n, latch);
            send(Protocol.INVALIDATE + " " + var + " " + n);
            try {
                boolean ok = latch.await(3, TimeUnit.SECONDS);
                log("INVALIDATE " + var + " -> " + id + (ok ? " (confirmado)" : " (SEM resposta)"));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                pending.remove(n);
            }
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                String line;
                while ((line = in.readLine()) != null) {
                    final String msg = line;
                    String[] p = msg.split(" ", 3);
                    if (p[0].equals(Protocol.ACK)) {
                        CountDownLatch c = pending.get(Long.parseLong(p[1]));
                        if (c != null) c.countDown();
                    } else if (p[0].equals(Protocol.HELLO)) {
                        if (clients.putIfAbsent(p[1], this) != null) {
                            send(Protocol.ERROR + " id em uso: " + p[1]);
                            break;
                        }
                        id = p[1];
                        registered = true;
                        send(Protocol.WELCOME + " " + id);
                        log("nó " + id + " conectado");
                    } else if (p[0].equals(Protocol.BYE)) {
                        break;
                    } else {
                        // Requisições rodam em outra thread para este laço continuar lendo ACKs.
                        pool.submit(() -> {
                            try { handle(msg); }
                            catch (Exception ex) { send(Protocol.ERROR + " " + ex); }
                        });
                    }
                }
            } catch (IOException ignored) {
            } finally {
                disconnect();
            }
        }

        private void handle(String msg) {
            String[] p = msg.split(" ", 3);
            switch (p[0]) {
                case Protocol.READ:
                    doRead(this, p[1]);
                    break;
                case Protocol.WRITE:
                    doWrite(this, p[1], p.length > 2 ? p[2] : "");
                    break;
                case Protocol.LOCK:
                    try { locks.lock(p[1], id); } catch (InterruptedException e) { return; }
                    if (closed) { locks.unlock(p[1], id); return; }
                    log("LOCK   " + p[1] + " concedida a " + id);
                    send(Protocol.LOCKED + " " + p[1]);
                    break;
                case Protocol.UNLOCK:
                    locks.unlock(p[1], id);
                    log("UNLOCK " + p[1] + " liberada por " + id);
                    send(Protocol.UNLOCKED + " " + p[1]);
                    break;
                default:
                    send(Protocol.ERROR + " comando desconhecido: " + p[0]);
            }
        }

        private void disconnect() {
            closed = true;
            if (registered) {
                clients.remove(id, this);
                for (Entry e : memory.values()) {
                    synchronized (e) { e.copyset.remove(id); }
                }
                locks.releaseAll(id);
                log("nó " + id + " desconectado");
            }
            try { socket.close(); } catch (IOException ignored) { }
        }
    }
}
