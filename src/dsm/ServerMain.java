package dsm;

/** Inicia o coordenador DSM.  Uso: java -cp out dsm.ServerMain [porta] */
public class ServerMain {
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : Protocol.DEFAULT_PORT;
        new DsmServer(port, true).start();
    }
}
