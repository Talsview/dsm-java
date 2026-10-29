package dsm;

import java.util.HashMap;
import java.util.Map;

/**
 * Gerenciador de travas nomeadas (exclusão mútua distribuída).
 * Fica no coordenador. Cada trava tem no máximo um dono por vez;
 * quem pede uma trava ocupada espera até ela ser liberada.
 */
public class LockManager {
    private final Map<String, String> owners = new HashMap<>();

    public synchronized void lock(String name, String nodeId) throws InterruptedException {
        while (owners.containsKey(name) && !owners.get(name).equals(nodeId)) {
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

    /** Libera todas as travas de um nó que caiu, para não travar os demais. */
    public synchronized void releaseAll(String nodeId) {
        if (owners.values().removeIf(nodeId::equals)) {
            notifyAll();
        }
    }
}
