package echo.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Contrato remoto usado entre servidores para coordenação interna:
 * heartbeat, eleição em anel (Chang-Roberts) e transferência de estado (join).
 */
public interface ReplicaControl extends Remote {

    /** Prefixo usado para registrar cada servidor no {@code rmiregistry} ({@code EchoServer-<id>}). */
    String PREFIX = "EchoServer-";

    /**
     * Retorna o identificador numérico único deste servidor no anel.
     *
     * @return Identificador do servidor (informado via {@code --id}).
     * @throws RemoteException Se ocorrer falha na comunicação RMI.
     */
    int getId() throws RemoteException;

    /**
     * Verifica se o servidor está ativo (heartbeat). Se a chamada falhar ou estourar
     * o timeout de RMI, o detector de falhas contabiliza uma falha consecutiva.
     *
     * @return {@code true} enquanto o servidor estiver vivo e respondendo.
     * @throws RemoteException Se o servidor estiver inacessível ou a chamada exceder o timeout.
     */
    boolean ping() throws RemoteException;

    /**
     * Entrega uma mensagem {@code ELECTION} do algoritmo de Chang-Roberts a este nó.
     * O processamento ocorre de forma assíncrona na thread de eleição do destinatário.
     *
     * @param candidateId Maior identificador visto até agora nesta volta da eleição.
     * @param initiatorId Identificador do servidor que iniciou esta rodada de eleição.
     * @throws RemoteException Se ocorrer falha na comunicação RMI ao contatar este sucessor.
     */
    void election(int candidateId, int initiatorId) throws RemoteException;

    /**
     * Entrega uma mensagem {@code ELECTED} anunciando o vencedor da eleição no anel.
     * O processamento ocorre de forma assíncrona na thread de eleição do destinatário.
     *
     * @param masterId Identificador do novo mestre eleito (maior ID ativo).
     * @param initiatorId Identificador do nó que originou o anúncio da vitória.
     * @throws RemoteException Se ocorrer falha na comunicação RMI ao contatar este sucessor.
     */
    void elected(int masterId, int initiatorId) throws RemoteException;

    /**
     * Retorna uma cópia do histórico completo de mensagens deste servidor,
     * usada na sincronização inicial de uma réplica que entra no sistema (join).
     *
     * @return Lista de mensagens armazenadas, ordenadas por número de sequência.
     * @throws RemoteException Se ocorrer falha na comunicação RMI.
     */
    List<Message> getState() throws RemoteException;
}
