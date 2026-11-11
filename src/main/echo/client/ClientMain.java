package echo.client;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Scanner;

/**
 * Ponto de entrada do cliente: interface de linha de comando interativa
 * com os comandos {@code echo <texto>}, {@code list} e {@code quit}.
 */
public final class ClientMain {

    /**
     * Configura o timeout de resposta RMI do cliente, lê o endereço do {@code rmiregistry}
     * e executa o laço interativo de leitura de comandos no terminal.
     *
     * @param args Argumentos de linha de comando (opcionalmente {@code --registry host:porta}).
     * @throws Exception Se o endereço do registry for inválido ou a inicialização RMI falhar.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10000");

        String registryAddr = "localhost:1099";
        for (int i = 0; i < args.length; i += 2) {
            if ("--registry".equals(args[i])) {
                registryAddr = args[i + 1];
            } else {
                throw new IllegalArgumentException("uso: ClientMain [--registry host:porta]");
            }
        }

        String[] hp = registryAddr.split(":");
        EchoClient client = new EchoClient(hp[0], Integer.parseInt(hp[1]));
        System.out.printf("[CLIENT] registry=%s. Comandos: echo <texto> | list | quit%n", registryAddr);

        Scanner input = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            if (!input.hasNextLine()) {
                break;
            }
            String line = input.nextLine().trim();
            if (line.equals("quit")) {
                break;
            }
            try {
                if (line.startsWith("echo ")) {
                    System.out.println(client.echo(line.substring(5)));
                } else if (line.equals("list")) {
                    List<String> msgs = client.getListOfMsg();
                    for (int i = 0; i < msgs.size(); i++) {
                        System.out.printf("%d. %s%n", i + 1, msgs.get(i));
                    }
                    System.out.printf("(%d mensagens)%n", msgs.size());
                } else if (!line.isEmpty()) {
                    System.out.println("comandos: echo <texto> | list | quit");
                }
            } catch (RemoteException e) {
                System.out.println("[CLIENT] erro: " + e.getMessage());
            }
        }
    }
}
