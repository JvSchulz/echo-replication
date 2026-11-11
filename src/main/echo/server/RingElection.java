package echo.server;

import echo.common.ReplicaControl;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Coordenador de eleição em anel pelo algoritmo de Chang-Roberts (maior ID ativo vence).
 * Toda a lógica roda em uma única thread: as chamadas remotas {@code election} e {@code elected}
 * apenas enfileiram a mensagem e retornam imediatamente, evitando chamadas RMI aninhadas pelo anel.
 */
public class RingElection {

    /**
     * Ouvinte notificado sobre a conclusão de uma eleição no anel.
     */
    public interface Listener {
        /**
         * Notifica que o próprio servidor local venceu a eleição e deve assumir o papel de mestre.
         */
        void onElectedSelf();

        /**
         * Notifica que outro servidor venceu a eleição e passou a ser o novo mestre.
         *
         * @param masterId Identificador numérico do servidor eleito mestre.
         */
        void onNewMaster(int masterId);
    }

    static final long TIMEOUT_MS = 5000;

    private final int myId;
    private final Registry registry;
    private final Listener listener;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "election");
        t.setDaemon(true);
        return t;
    });

    private boolean participant = false;
    private ScheduledFuture<?> timeout;

    /**
     * Cria o coordenador de eleição em anel para o servidor {@code myId}.
     *
     * @param myId Identificador numérico deste servidor no anel.
     * @param registry Referência ao {@code rmiregistry} onde os membros do anel estão registrados.
     * @param listener Ouvinte notificado quando este nó vence ou quando outro nó é eleito mestre.
     */
    public RingElection(int myId, Registry registry, Listener listener) {
        this.myId = myId;
        this.registry = registry;
        this.listener = listener;
    }

    /**
     * Agenda na thread de eleição o início de uma nova rodada tendo este nó como iniciador.
     */
    public void startElection() {
        worker.execute(this::doStartElection);
    }

    /**
     * Enfileira na thread de eleição uma mensagem {@code ELECTION} recebida via RMI e retorna imediatamente.
     *
     * @param candidateId Maior identificador visto até agora na mensagem recebida.
     * @param initiatorId Identificador do servidor que iniciou esta eleição.
     */
    public void receiveElection(int candidateId, int initiatorId) {
        worker.execute(() -> handleElection(candidateId, initiatorId));
    }

    /**
     * Enfileira na thread de eleição uma mensagem {@code ELECTED} recebida via RMI e retorna imediatamente.
     *
     * @param masterId Identificador do novo mestre eleito.
     * @param initiatorId Identificador do servidor que originou o anúncio.
     */
    public void receiveElected(int masterId, int initiatorId) {
        worker.execute(() -> handleElected(masterId, initiatorId));
    }

    /**
     * Marca este servidor como participante, arma o timeout de segurança e envia {@code ELECTION(myId, myId)}
     * ao próximo sucessor vivo no anel.
     */
    private void doStartElection() {
        participant = true;
        armTimeout();
        log("iniciando eleição; anel: " + describeRing());
        send(true, myId, myId);
    }

    /**
     * Avalia uma mensagem {@code ELECTION} segundo a regra de Chang-Roberts na thread exclusiva da eleição:
     * repassa candidatos maiores, substitui candidatos menores por {@code myId} se ainda não participava
     * (ou descarta se já participava) e declara vitória quando {@code candidateId == myId}.
     *
     * @param candidateId Identificador do candidato contido na mensagem.
     * @param initiatorId Identificador do servidor que iniciou a rodada.
     */
    private void handleElection(int candidateId, int initiatorId) {
        String msg = "recebido ELECTION(candidato=" + candidateId + ", iniciador=" + initiatorId + ")";
        if (candidateId > myId) {
            participant = true;
            armTimeout();
            log(msg + " -> repassando candidato=" + candidateId);
            send(true, candidateId, initiatorId);
        } else if (candidateId < myId) {
            if (!participant) {
                participant = true;
                armTimeout();
                log(msg + " -> repassando candidato=" + myId);
                send(true, myId, initiatorId);
            } else {
                log(msg + " -> descartado (já propago candidato maior)");
            }
        } else {
            participant = false;
            cancelTimeout();
            log(msg + " -> meu ID deu a volta: venci");
            listener.onElectedSelf();
            send(false, myId, myId);
        }
    }

    /**
     * Processa uma mensagem {@code ELECTED} na thread exclusiva da eleição.
     *
     * Se a mensagem retornou ao próprio vencedor ({@code masterId == myId}), o anúncio completou a volta
     * no anel; caso contrário, encerra a participação deste nó, repassa o anúncio ao sucessor e notifica
     * o {@link Listener}.
     *
     * @param masterId Identificador numérico do mestre eleito.
     * @param initiatorId Identificador numérico de quem iniciou o anúncio.
     */
    private void handleElected(int masterId, int initiatorId) {
        if (masterId == myId) {
            log("ELECTED(mestre=" + masterId + ") completou a volta do anel");
            return;
        }
        participant = false;
        cancelTimeout();
        log("recebido ELECTED(mestre=" + masterId + ", iniciador=" + initiatorId + ") -> repassando");
        send(false, masterId, initiatorId);
        listener.onNewMaster(masterId);
    }

    /**
     * Envia uma mensagem de eleição ({@code ELECTION} ou {@code ELECTED}) ao próximo sucessor vivo no anel.
     *
     * Sucessores que falham na chamada RMI são removidos do registry via {@link #unbindDead(int)}
     * e o próximo sucessor é tentado imediatamente. Se o candidato ou mestre anunciado não estiver
     * mais registrado no anel, a mensagem é descartada para não circular indefinidamente.
     *
     * @param isElection {@code true} para enviar {@code ELECTION}; {@code false} para enviar {@code ELECTED}.
     * @param value Identificador do candidato (em {@code ELECTION}) ou do mestre eleito (em {@code ELECTED}).
     * @param initiatorId Identificador do servidor que iniciou a rodada ou o anúncio.
     */
    private void send(boolean isElection, int value, int initiatorId) {
        String kind = isElection ? "ELECTION" : "ELECTED";
        while (true) {
            TreeSet<Integer> liveIds;
            try {
                liveIds = liveIds();
            } catch (RemoteException e) {
                log("registry inacessível: " + e.getMessage());
                return;
            }
            if (value != myId && !liveIds.contains(value)) {
                log("S" + value + " saiu do anel: " + kind + "(" + value + ") descartada");
                return;
            }
            liveIds.add(myId);
            Integer higher = liveIds.higher(myId);
            int next = higher != null ? higher : liveIds.first();
            if (next == myId) {
                if (isElection) {
                    handleElection(value, initiatorId);
                } else {
                    handleElected(value, initiatorId);
                }
                return;
            }
            try {
                ReplicaControl successor = (ReplicaControl) registry.lookup(ReplicaControl.PREFIX + next);
                if (isElection) {
                    successor.election(value, initiatorId);
                } else {
                    successor.elected(value, initiatorId);
                }
                log("enviado " + kind + "(" + value + ", iniciador=" + initiatorId + ") para S" + next);
                return;
            } catch (RemoteException | NotBoundException e) {
                log("sucessor S" + next + " não responde; pulando");
                unbindDead(next);
            }
        }
    }

    /**
     * Consulta o {@code rmiregistry} e retorna o conjunto ordenado de identificadores de todos os
     * servidores registrados sob o prefixo {@link ReplicaControl#PREFIX}.
     *
     * @return Conjunto ordenado em ordem crescente dos IDs registrados no serviço de nomes.
     * @throws RemoteException Se o {@code rmiregistry} estiver inacessível.
     */
    public TreeSet<Integer> liveIds() throws RemoteException {
        TreeSet<Integer> ids = new TreeSet<>();
        for (String name : registry.list()) {
            if (name.startsWith(ReplicaControl.PREFIX)) {
                try {
                    ids.add(Integer.parseInt(name.substring(ReplicaControl.PREFIX.length())));
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return ids;
    }

    /**
     * Remove do {@code rmiregistry} o registro de um servidor que deixou de responder.
     *
     * @param nodeId Identificador numérico do servidor inativo a desregistrar.
     */
    public void unbindDead(int nodeId) {
        String name = ReplicaControl.PREFIX + nodeId;
        try {
            registry.unbind(name);
            System.out.printf("[S%d][RMI] %s removido do registry%n", myId, name);
        } catch (NotBoundException | RemoteException ignored) {
        }
    }

    /**
     * Formata a lista de membros atualmente registrados no anel para exibição em logs.
     *
     * @return Representação textual dos IDs ativos, {@code "vazio"} ou {@code "registry inacessível"}.
     */
    public String describeRing() {
        try {
            TreeSet<Integer> ids = liveIds();
            return ids.isEmpty() ? "vazio" : ids.toString();
        } catch (RemoteException e) {
            return "registry inacessível";
        }
    }

    /**
     * Reinicia o temporizador de segurança de {@link #TIMEOUT_MS} para reiniciar a eleição
     * caso nenhuma mensagem {@code ELECTED} seja recebida no prazo.
     */
    private void armTimeout() {
        cancelTimeout();
        timeout = worker.schedule(() -> {
            timeout = null;
            if (participant) {
                log("nenhum ELECTED em " + TIMEOUT_MS + " ms; reiniciando eleição");
                doStartElection();
            }
        }, TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Cancela o temporizador de segurança da eleição atual, caso esteja agendado.
     */
    private void cancelTimeout() {
        if (timeout != null) {
            timeout.cancel(false);
            timeout = null;
        }
    }

    /**
     * Imprime uma mensagem de log da área {@code ELECTION} para este servidor.
     *
     * @param text Mensagem a registrar na saída padrão.
     */
    private void log(String text) {
        System.out.printf("[S%d][ELECTION] %s%n", myId, text);
    }
}
