package echo.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Contrato remoto usado pelos clientes. Apenas o servidor mestre atende estas chamadas;
 * servidores que atuam como réplica recusam com {@link RemoteException}.
 */
public interface EchoService extends Remote {

    /** Nome bem conhecido do mestre atual no serviço de nomes (rmiregistry). */
    String MASTER_NAME = "EchoMaster";

    /**
     * Registra uma mensagem no histórico, replica para as demais réplicas via MQTT
     * e devolve o próprio texto ao cliente.
     *
     * @param msg Texto enviado pelo cliente para ser ecoado e armazenado.
     * @return O mesmo texto {@code msg} recebido, após gravação local e publicação no broker.
     * @throws RemoteException Se ocorrer falha de comunicação RMI, se este nó não for o mestre
     *     ou se a publicação no broker MQTT falhar.
     */
    String echo(String msg) throws RemoteException;

    /**
     * Retorna a lista completa de textos de mensagens já processadas pelo serviço,
     * na ordem de número de sequência atribuído pelo mestre.
     *
     * @return Lista ordenada com o texto de cada mensagem armazenada no histórico.
     * @throws RemoteException Se ocorrer falha de comunicação RMI ou se este nó não for o mestre.
     */
    List<String> getListOfMsg() throws RemoteException;
}
