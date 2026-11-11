package echo.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Ponto de entrada do processo servidor. O mesmo código executa em todas as réplicas;
 * cada instância é identificada pelo argumento obrigatório {@code --id}.
 */
public final class ServerMain {
    private static ReplicaServer server;

    /**
     * Define os timeouts globais de chamada RMI, interpreta os argumentos de linha de comando,
     * instancia {@link ReplicaServer} e inicia o protocolo de entrada no sistema.
     *
     * @param args Argumentos de linha de comando ({@code --id}, {@code --registry}, {@code --broker}, {@code --topic}).
     * @throws Exception Se os argumentos forem inválidos ou se a conexão inicial com o registry ou broker falhar.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");
        System.setProperty("sun.rmi.transport.proxy.connectTimeout", "1000");

        Integer id = null;
        String registryAddr = "localhost:1099";
        String broker = "tcp://localhost:1883";
        String topic = "echo/replication";

        for (int i = 0; i < args.length; i += 2) {
            String val = args[i + 1];
            switch (args[i]) {
                case "--id":
                    id = Integer.parseInt(val);
                    break;
                case "--registry":
                    registryAddr = val;
                    break;
                case "--broker":
                    broker = val;
                    break;
                case "--topic":
                    topic = val;
                    break;
                default:
                    throw new IllegalArgumentException("argumento desconhecido: " + args[i]);
            }
        }
        if (id == null) {
            throw new IllegalArgumentException(
                    "uso: ServerMain --id N [--registry host:porta] [--broker url] [--topic t]");
        }

        String[] hp = registryAddr.split(":");
        Registry registry = LocateRegistry.getRegistry(hp[0], Integer.parseInt(hp[1]));
        server = new ReplicaServer(id, registry, broker, topic);
        server.start();
    }
}
