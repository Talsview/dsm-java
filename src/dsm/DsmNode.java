package dsm;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nó DSM: é o que cada processo Java usa para enxergar a memória compartilhada.
 *
 * Cada nó mantém uma CÓPIA LOCAL (cache) das variáveis que já leu ou escreveu:
 *   - read: se a cópia local é válida, responde sem usar a rede (acerto);
 *           senão pede o valor ao coordenador (falha) e guarda a cópia;
 *   - write: envia ao coordenador, que invalida as cópias dos outros nós;
 *   - INVALIDATE recebido: descarta a cópia local e confirma (ACK).
 * Uma thread de leitura trata as mensagens do coordenador na ordem em que chegam.
 */
public class DsmNode implements SharedMemory, Closeable {

    private static final class CacheEntry {
        final long version;
        final String value;
        CacheEntry(long version, String value) { this.version = version; this.value = value; }
    }

    private final String id;
    private final Socket socket;
    private final PrintWriter out;
    private final Object outLock = new Object();
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final BlockingQueue<String[]> responses = new LinkedBlockingQueue<>();
    private final AtomicInteger hits = new AtomicInteger();
    private final AtomicInteger misses = new AtomicInteger();
    private final AtomicInteger invalidations = new AtomicInteger();
    private final AtomicInteger messagesSent = new AtomicInteger();
    private volatile boolean verbose = false;

    public DsmNode(String id, String host, int port) throws IOException {
        Protocol.validateName(id);
        this.id = id;
        this.socket = new Socket(host, port);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
        send(Protocol.HELLO + " " + id);
        String welcome = in.readLine();
        if (welcome == null || !welcome.startsWith(Protocol.WELCOME)) {
            socket.close();
            throw new IOException("Coordenador recusou a conexão: " + welcome);
        }
        Thread reader = new Thread(() -> readerLoop(in), "dsm-node-reader-" + id);
        reader.setDaemon(true);
        reader.start();
    }

    public String getId() { return id; }
    public void setVerbose(boolean v) { this.verbose = v; }

    private void send(String line) {
        synchronized (outLock) { out.println(line); }
        messagesSent.incrementAndGet();
    }

    /** Trata mensagens do coordenador, uma a uma, na ordem de chegada. */
    private void readerLoop(BufferedReader in) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                String[] p = line.split(" ", 4);
                switch (p[0]) {
                    case Protocol.VALUE:
                    case Protocol.WROTE:
                        // a cópia local é guardada aqui, antes de qualquer INVALIDATE posterior
                        cache.put(p[1], new CacheEntry(Long.parseLong(p[2]), p.length > 3 ? p[3] : ""));
                        responses.offer(p);
                        break;
                    case Protocol.INVALIDATE:
                        cache.remove(p[1]);
                        invalidations.incrementAndGet();
                        if (verbose) System.out.println("   [" + id + "] cópia local de '" + p[1] + "' invalidada");
                        send(Protocol.ACK + " " + p[2]);
                        break;
                    default:
                        responses.offer(p); // LOCKED, UNLOCKED, ERROR
                }
            }
        } catch (IOException ignored) {
        } finally {
            responses.offer(new String[]{"CLOSED"});
        }
    }

    private String[] await() throws IOException {
        try {
            String[] r = responses.take();
            if (r[0].equals(Protocol.ERROR)) throw new IOException("Erro do coordenador: " + String.join(" ", r));
            if (r[0].equals("CLOSED")) throw new IOException("Conexão com o coordenador encerrada");
            return r;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrompido", e);
        }
    }

    // ---------------------------------------------------------------- API SharedMemory

    @Override
    public synchronized String read(String var) throws IOException {
        Protocol.validateName(var);
        CacheEntry c = cache.get(var);
        if (c != null) {                      // acerto: sem comunicação em rede
            hits.incrementAndGet();
            return c.value;
        }
        misses.incrementAndGet();             // falha: busca no coordenador
        send(Protocol.READ + " " + var);
        String[] r = await();                 // VALUE <var> <versao> <valor>
        return r.length > 3 ? r[3] : "";
    }

    @Override
    public synchronized void write(String var, String value) throws IOException {
        Protocol.validateName(var);
        if (value.indexOf('\n') >= 0) throw new IllegalArgumentException("Valor não pode ter quebra de linha");
        send(Protocol.WRITE + " " + var + " " + value);
        await();
    }

    @Override
    public synchronized void lock(String name) throws IOException {
        Protocol.validateName(name);
        send(Protocol.LOCK + " " + name);
        await();
    }

    @Override
    public synchronized void unlock(String name) throws IOException {
        Protocol.validateName(name);
        send(Protocol.UNLOCK + " " + name);
        await();
    }

    // ---------------------------------------------------------------- observação (para a demonstração)

    /** Cópias locais atuais: variável -> "valor (vN)". */
    public Map<String, String> cacheSnapshot() {
        Map<String, String> m = new TreeMap<>();
        cache.forEach((k, v) -> m.put(k, v.value + " (v" + v.version + ")"));
        return m;
    }

    public String stats() {
        return "acertos de cache=" + hits + ", falhas=" + misses
                + ", invalidações recebidas=" + invalidations + ", mensagens enviadas=" + messagesSent;
    }

    @Override
    public void close() {
        try { send(Protocol.BYE); socket.close(); } catch (IOException ignored) { }
    }
}
