package echo.client;

import echo.common.EchoService;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

/**
 * Cliente RMI do serviço de echo com transparência de falha: se o mestre atual
 * cair ou recusar a chamada, o stub em cache é descartado e o cliente procura
 * o novo mestre no {@code rmiregistry} com backoff exponencial.
 */
public class EchoClient {
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final long MAX_BACKOFF_MS = 5000;
    private static final long TOTAL_BUDGET_MS = 30000;

    /**
     * Representa uma operação remota executada contra o stub do servidor mestre.
     *
     * @param <T> Tipo do valor retornado pela chamada remota.
     */
    private interface Operation<T> {
        /**
         * Executa a chamada remota sobre o stub do mestre informado.
         *
         * @param master Stub remoto {@link EchoService} do mestre atual.
         * @return Resultado produzido pela chamada remota.
         * @throws RemoteException Se a comunicação com o mestre falhar.
         */
        T run(EchoService master) throws RemoteException;
    }

    private final Registry registry;
    private EchoService cachedMaster;

    /**
     * Cria o cliente RMI conectado ao {@code rmiregistry} no endereço informado.
     *
     * @param host Nome ou IP da máquina onde roda o {@code rmiregistry}.
     * @param port Porta TCP do {@code rmiregistry}.
     * @throws RemoteException Se a referência ao {@code rmiregistry} não puder ser criada.
     */
    public EchoClient(String host, int port) throws RemoteException {
        this.registry = LocateRegistry.getRegistry(host, port);
    }

    /**
     * Envia um texto ao mestre para ser ecoado e replicado, reconectando automaticamente
     * ao novo mestre caso o atual falhe durante a operação.
     *
     * @param text Texto a enviar ao serviço de echo.
     * @return O texto ecoado pelo servidor mestre.
     * @throws RemoteException Se nenhum mestre responder dentro do orçamento de tempo ({@link #TOTAL_BUDGET_MS}).
     */
    public String echo(String text) throws RemoteException {
        return withFailover(master -> master.echo(text));
    }

    /**
     * Solicita ao mestre o histórico completo de mensagens do serviço, com failover transparente.
     *
     * @return Lista ordenada com os textos de todas as mensagens registradas.
     * @throws RemoteException Se nenhum mestre responder dentro do orçamento de tempo ({@link #TOTAL_BUDGET_MS}).
     */
    public List<String> getListOfMsg() throws RemoteException {
        return withFailover(EchoService::getListOfMsg);
    }

    /**
     * Executa uma operação remota no mestre em cache e, em caso de falha, invalida o cache
     * e repete o {@code lookup} de {@link EchoService#MASTER_NAME} com backoff exponencial
     * (500 ms até 5 s) dentro do orçamento total de 30 s.
     *
     * @param <T> Tipo de retorno da operação remota.
     * @param operation Função que invoca o método desejado no stub {@link EchoService}.
     * @return Resultado devolvido pelo mestre após a execução bem-sucedida de {@code operation}.
     * @throws RemoteException Se o orçamento total de tempo ({@link #TOTAL_BUDGET_MS}) se esgotar
     *     ou se a thread for interrompida durante a espera.
     */
    private <T> T withFailover(Operation<T> operation) throws RemoteException {
        long deadline = System.currentTimeMillis() + TOTAL_BUDGET_MS;
        long backoff = INITIAL_BACKOFF_MS;
        boolean warned = false;
        while (true) {
            try {
                if (cachedMaster == null) {
                    cachedMaster = (EchoService) registry.lookup(EchoService.MASTER_NAME);
                }
                return operation.run(cachedMaster);
            } catch (RemoteException | NotBoundException e) {
                cachedMaster = null;
                if (System.currentTimeMillis() + backoff > deadline) {
                    throw new RemoteException("serviço indisponível após 30 s: " + e.getMessage());
                }
                if (!warned) {
                    System.out.println("[CLIENT] reconectando ao serviço...");
                    warned = true;
                }
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RemoteException("interrompido enquanto aguardava o novo mestre");
                }
                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        }
    }
}
