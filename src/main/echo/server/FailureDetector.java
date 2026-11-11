package echo.server;

import echo.common.ReplicaControl;

import java.rmi.RemoteException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

/**
 * Detector de falhas por heartbeat: invoca {@code ping()} no mestre periodicamente e,
 * após falhas consecutivas, declara o mestre suspeito de falha.
 */
public class FailureDetector {
    static final long INTERVAL_MS = 1000;
    static final int MAX_FAILURES = 3;

    private final int myId;
    private final IntConsumer onSuspected;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "heartbeat");
        t.setDaemon(true);
        return t;
    });

    private ScheduledFuture<?> task;
    private int failures;

    /**
     * Cria um detector de falhas para o servidor {@code myId}.
     *
     * @param myId Identificador numérico deste servidor, usado na formatação dos logs.
     * @param onSuspected Callback invocado (recebendo o {@code masterId}) quando o mestre
     *     atinge {@link #MAX_FAILURES} falhas consecutivas de {@code ping()}.
     */
    public FailureDetector(int myId, IntConsumer onSuspected) {
        this.myId = myId;
        this.onSuspected = onSuspected;
    }

    /**
     * Inicia (ou substitui) o monitoramento periódico do mestre informado, zerando o contador de falhas.
     *
     * @param masterId Identificador numérico do servidor mestre a monitorar.
     * @param master Stub remoto {@link ReplicaControl} do mestre onde {@code ping()} será chamado.
     */
    public synchronized void watch(int masterId, ReplicaControl master) {
        stop();
        failures = 0;
        task = scheduler.scheduleWithFixedDelay(() -> check(masterId, master),
                INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Interrompe o agendamento atual de verificações de heartbeat.
     */
    public synchronized void stop() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
    }

    /**
     * Executa uma verificação periódica de {@code ping()} contra o mestre monitorado.
     *
     * Se o mestre responder, zera o contador de falhas consecutivas; se atingir {@link #MAX_FAILURES}
     * falhas seguidas, interrompe o detector e dispara {@code onSuspected} fora do bloco sincronizado.
     *
     * @param masterId Identificador numérico do mestre monitorado.
     * @param master Stub remoto do mestre.
     */
    private void check(int masterId, ReplicaControl master) {
        try {
            if (master.ping()) {
                synchronized (this) {
                    failures = 0;
                }
                return;
            }
        } catch (RemoteException ignored) {
        }
        synchronized (this) {
            if (task == null) {
                return;
            }
            failures++;
            System.out.printf("[S%d][HEARTBEAT] ping em S%d falhou (%d/%d)%n",
                    myId, masterId, failures, MAX_FAILURES);
            if (failures < MAX_FAILURES) {
                return;
            }
            stop();
        }
        System.out.printf("[S%d][HEARTBEAT] mestre S%d suspeito de falha%n", myId, masterId);
        onSuspected.accept(masterId);
    }
}
