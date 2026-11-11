package echo.server;

import echo.common.EchoService;
import echo.common.Message;
import echo.common.ReplicaControl;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

/**
 * Servidor de réplica do serviço de echo. Implementa as interfaces remotas de cliente
 * ({@link EchoService}) e de coordenação entre servidores ({@link ReplicaControl}),
 * alternando entre o papel de réplica (assina o tópico MQTT e monitora o mestre)
 * e o papel de mestre (atende clientes e publica no broker MQTT).
 */
public class ReplicaServer extends UnicastRemoteObject
        implements EchoService, ReplicaControl, RingElection.Listener {
    private static final long serialVersionUID = 1L;

    private final int id;
    private final transient Registry registry;
    private final transient MessageStore store = new MessageStore();
    private final transient MqttReplicator replicator;
    private final transient FailureDetector detector;
    private final transient RingElection election;
    private final transient Object echoLock = new Object();
    private volatile int masterId = -1;
    private volatile boolean master = false;

    /**
     * Exporta o objeto remoto RMI e inicializa os subsistemas de replicação MQTT, heartbeat e eleição.
     *
     * @param id Identificador numérico único da réplica no anel ({@code --id}).
     * @param registry Referência ao {@code rmiregistry} onde os serviços remotos são registrados.
     * @param brokerUrl URI do broker MQTT (ex.: {@code tcp://localhost:1883}).
     * @param topic Nome do tópico MQTT de replicação (ex.: {@code echo/replication}).
     * @throws RemoteException Se a exportação do {@link UnicastRemoteObject} falhar.
     * @throws MqttException Se o cliente MQTT não puder ser criado.
     */
    public ReplicaServer(int id, Registry registry, String brokerUrl, String topic)
            throws RemoteException, MqttException {
        super();
        this.id = id;
        this.registry = registry;
        this.replicator = new MqttReplicator(id, brokerUrl, topic, store);
        this.detector = new FailureDetector(id, this::onMasterSuspected);
        this.election = new RingElection(id, registry, this);
    }

    /**
     * Executa o protocolo de entrada no sistema: registra {@code EchoServer-<id>}, conecta ao broker MQTT,
     * assina o tópico com buffer ativo e passa a seguir o mestre atual ou inicia uma eleição caso não
     * exista mestre vivo.
     *
     * @throws RemoteException Se o registro no {@code rmiregistry} ou a consulta inicial falhar.
     * @throws MqttException Se a conexão ou inscrição inicial no broker MQTT falhar.
     */
    public void start() throws RemoteException, MqttException {
        String myName = PREFIX + id;
        registry.rebind(myName, this);
        log("RMI", "registrado como " + myName);
        replicator.connect();
        replicator.subscribe();
        log("JOIN", "anel atual: " + election.describeRing());

        Integer currentMaster = findMasterId();
        if (currentMaster == null) {
            log("JOIN", "nenhum mestre vivo; iniciando eleição");
            election.startElection();
        } else {
            followMaster(currentMaster);
        }
    }

    /**
     * Descobre o identificador do mestre ativo consultando o nome {@link EchoService#MASTER_NAME}
     * no {@code rmiregistry}.
     *
     * @return Identificador numérico do mestre vivo, ou {@code null} se {@code EchoMaster} não estiver
     *     registrado ou apontar para um processo que já parou de responder.
     */
    private Integer findMasterId() {
        try {
            ReplicaControl current = (ReplicaControl) registry.lookup(MASTER_NAME);
            return current.getId();
        } catch (NotBoundException | RemoteException e) {
            return null;
        }
    }

    /**
     * Configura esta réplica para seguir o mestre indicado: sincroniza o histórico inicial via
     * {@code getState()} (se ainda estiver em modo de bufferização) e inicia o detector de falhas
     * por heartbeat. Se o mestre indicado estiver inacessível, dispara uma nova eleição.
     *
     * @param newMasterId Identificador numérico do servidor mestre a seguir.
     */
    private synchronized void followMaster(int newMasterId) {
        masterId = newMasterId;
        try {
            ReplicaControl masterStub = (ReplicaControl) registry.lookup(PREFIX + newMasterId);
            if (replicator.isBuffering()) {
                List<Message> state = masterStub.getState();
                store.addAll(state);
                int buffered = replicator.stopBuffering();
                log("JOIN", "histórico de S" + newMasterId + ": " + state.size()
                        + " mensagens + " + buffered + " do buffer -> total=" + store.size());
            }
            detector.watch(newMasterId, masterStub);
            log("HEARTBEAT", "monitorando o mestre S" + newMasterId);
        } catch (RemoteException | NotBoundException e) {
            log("JOIN", "mestre S" + newMasterId + " inacessível; iniciando eleição");
            election.startElection();
        }
    }

    /**
     * Reage à suspeita de falha do mestre reportada pelo {@link FailureDetector}:
     * remove o mestre suspeito do {@code rmiregistry} e inicia uma eleição em anel.
     *
     * @param suspectId Identificador numérico do mestre que falhou nos heartbeats consecutivos.
     */
    private void onMasterSuspected(int suspectId) {
        if (suspectId != masterId) {
            return;
        }
        election.unbindDead(suspectId);
        election.startElection();
    }

    /**
     * Promove este servidor a mestre ao vencer a eleição: encerra o buffer inicial,
     * faz {@code unsubscribe} do tópico MQTT, interrompe o detector de falhas, registra
     * {@link EchoService#MASTER_NAME} no {@code rmiregistry} e habilita o atendimento a clientes.
     */
    @Override
    public synchronized void onElectedSelf() {
        replicator.stopBuffering();
        try {
            replicator.unsubscribe();
        } catch (MqttException e) {
            log("MQTT", "falha no unsubscribe: " + e.getMessage());
        }
        detector.stop();
        try {
            registry.rebind(MASTER_NAME, this);
        } catch (RemoteException e) {
            log("RMI", "falha ao registrar " + MASTER_NAME + ": " + e.getMessage());
        }
        masterId = id;
        master = true;
        log("ELECTION", "agora sou o mestre (" + MASTER_NAME + "); próximo seq="
                + store.nextSeq() + ", histórico=" + store.size());
    }

    /**
     * Atualiza o monitoramento deste servidor quando outro nó vence a eleição.
     *
     * @param newMasterId Identificador numérico do novo mestre eleito.
     */
    @Override
    public synchronized void onNewMaster(int newMasterId) {
        followMaster(newMasterId);
    }

    /**
     * Processa uma requisição {@code echo} de um cliente: grava localmente com novo {@code seq},
     * publica no broker MQTT aguardando confirmação QoS 1 e devolve o texto ao cliente.
     *
     * @param msg Texto enviado pelo cliente.
     * @return O próprio texto {@code msg} após gravação local e publicação no broker.
     * @throws RemoteException Se este nó não for o mestre ou se a publicação MQTT no broker falhar.
     */
    @Override
    public String echo(String msg) throws RemoteException {
        if (!master) {
            throw new RemoteException("este servidor não é o mestre");
        }
        synchronized (echoLock) {
            Message message = store.appendNew(id, msg);
            log("RMI", "echo seq=" + message.seq + " \"" + msg + "\" (total=" + store.size() + ")");
            try {
                replicator.publish(message);
            } catch (MqttException e) {
                throw new RemoteException("falha ao replicar a mensagem: " + e.getMessage());
            }
        }
        return msg;
    }

    /**
     * Retorna ao cliente a lista ordenada dos textos de todas as mensagens armazenadas no mestre.
     *
     * @return Lista com o texto de cada mensagem na ordem de sequência.
     * @throws RemoteException Se este servidor não for o mestre.
     */
    @Override
    public List<String> getListOfMsg() throws RemoteException {
        if (!master) {
            throw new RemoteException("este servidor não é o mestre");
        }
        List<String> texts = store.texts();
        log("RMI", "getListOfMsg -> " + texts.size() + " mensagens");
        return texts;
    }

    /**
     * Retorna o identificador numérico único deste servidor.
     *
     * @return Identificador configurado em {@code --id}.
     */
    @Override
    public int getId() {
        return id;
    }

    /**
     * Responde ao heartbeat periódico enviado pelas réplicas para confirmar que este nó está ativo.
     *
     * @return Sempre {@code true}.
     */
    @Override
    public boolean ping() {
        return true;
    }

    /**
     * Recebe via RMI uma mensagem {@code ELECTION} do predecessor no anel e a entrega para
     * processamento assíncrono em {@link RingElection}.
     *
     * @param candidateId Maior identificador visto até agora na rodada de eleição.
     * @param initiatorId Identificador do servidor que iniciou a rodada.
     */
    @Override
    public void election(int candidateId, int initiatorId) {
        election.receiveElection(candidateId, initiatorId);
    }

    /**
     * Recebe via RMI uma mensagem {@code ELECTED} do predecessor no anel e a entrega para
     * processamento assíncrono em {@link RingElection}.
     *
     * @param newMasterId Identificador do novo mestre eleito.
     * @param initiatorId Identificador do servidor que originou o anúncio.
     */
    @Override
    public void elected(int newMasterId, int initiatorId) {
        election.receiveElected(newMasterId, initiatorId);
    }

    /**
     * Retorna uma cópia da lista de mensagens armazenadas para sincronização de novas réplicas (join).
     *
     * @return Lista de {@link Message} em ordem de sequência.
     */
    @Override
    public List<Message> getState() {
        return store.snapshot();
    }

    /**
     * Imprime na saída padrão uma linha de log identificada pelo servidor e pela área do subsistema.
     *
     * @param area Área funcional do evento ({@code RMI}, {@code MQTT}, {@code HEARTBEAT}, {@code ELECTION} ou {@code JOIN}).
     * @param text Mensagem a registrar.
     */
    private void log(String area, String text) {
        System.out.printf("[S%d][%s] %s%n", id, area, text);
    }
}
