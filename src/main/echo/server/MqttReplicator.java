package echo.server;

import echo.common.Message;
import com.google.gson.Gson;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Componente de comunicação indireta via broker MQTT.
 * O mestre publica mensagens no tópico de replicação; as réplicas assinam o tópico
 * e recebem as notificações correspondentes.
 */
public class MqttReplicator implements MqttCallbackExtended {
    private static final int QOS = 1;
    private static final long PUBLISH_TIMEOUT_MS = 5000;

    private final int id;
    private final String topic;
    private final MessageStore store;
    private final MqttClient client;
    private final Gson gson = new Gson();
    private final List<Message> buffer = new ArrayList<>();
    private boolean buffering = true;
    private volatile boolean subscriber = false;

    /**
     * Cria o cliente MQTT síncrono com persistência em memória e timeout de publicação.
     *
     * @param id Identificador numérico único da réplica, usado no {@code clientId} e nos logs.
     * @param brokerUrl URI do broker MQTT (ex.: {@code tcp://localhost:1883}).
     * @param topic Nome do tópico MQTT de replicação (ex.: {@code echo/replication}).
     * @param store Repositório local de mensagens onde as notificações recebidas serão gravadas.
     * @throws MqttException Se o cliente Paho não puder ser instanciado com a URI informada.
     */
    public MqttReplicator(int id, String brokerUrl, String topic, MessageStore store) throws MqttException {
        this.id = id;
        this.topic = topic;
        this.store = store;
        this.client = new MqttClient(brokerUrl, "echo-server-" + id, new MemoryPersistence());
        this.client.setTimeToWait(PUBLISH_TIMEOUT_MS);
        this.client.setCallback(this);
    }

    /**
     * Abre a conexão com o broker MQTT usando sessão limpa ({@code cleanSession=true}) e reconexão automática.
     *
     * @throws MqttException Se o broker estiver inacessível ou recusar a conexão.
     */
    public void connect() throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        client.connect(options);
        log("conectado ao broker " + client.getServerURI());
    }

    /**
     * Serializa a mensagem em JSON e a publica no tópico de replicação com QoS 1.
     *
     * Como {@link MqttClient} é síncrono, a chamada bloqueia até o recebimento do {@code PUBACK}
     * do broker (ou até {@code PUBLISH_TIMEOUT_MS}), garantindo que o mestre só responda ao
     * cliente depois que a mensagem foi entregue ao broker.
     *
     * @param msg Mensagem já gravada no histórico local do mestre.
     * @throws MqttException Se a publicação falhar ou exceder o tempo limite de espera do QoS 1.
     */
    public void publish(Message msg) throws MqttException {
        MqttMessage mqttMsg = new MqttMessage(gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
        mqttMsg.setQos(QOS);
        client.publish(topic, mqttMsg);
        log("publicado seq=" + msg.seq + " em " + topic);
    }

    /**
     * Assina o tópico de replicação com QoS 1 para passar a receber as publicações do mestre.
     *
     * @throws MqttException Se a inscrição no broker falhar.
     */
    public void subscribe() throws MqttException {
        client.subscribe(topic, QOS);
        subscriber = true;
        log("inscrito em " + topic);
    }

    /**
     * Cancela a assinatura do tópico de replicação ao ser promovido a mestre, evitando que
     * o novo mestre receba de volta pelo broker as mensagens que ele próprio publicar.
     *
     * @throws MqttException Se o cancelamento da inscrição no broker falhar.
     */
    public void unsubscribe() throws MqttException {
        subscriber = false;
        client.unsubscribe(topic);
        log("unsubscribe de " + topic);
    }

    /**
     * Indica se este replicador ainda está acumulando notificações no buffer de entrada (join).
     *
     * @return {@code true} enquanto a réplica ainda não concluiu a cópia inicial de estado do mestre;
     *     {@code false} quando já grava diretamente no {@link MessageStore}.
     */
    public synchronized boolean isBuffering() {
        return buffering;
    }

    /**
     * Encerra o modo de bufferização inicial, mesclando todas as notificações retidas no {@link MessageStore}
     * e passando a gravar novas notificações diretamente no histórico.
     *
     * @return Quantidade de notificações que estavam retidas no buffer durante a cópia de estado.
     */
    public synchronized int stopBuffering() {
        int count = buffer.size();
        store.addAll(buffer);
        buffer.clear();
        buffering = false;
        return count;
    }

    /**
     * Recebe do Paho uma mensagem publicada no tópico assinado, desserializa o JSON e a entrega
     * ao buffer de entrada (se ainda estiver sincronizando) ou diretamente ao {@link MessageStore}.
     *
     * @param fromTopic Nome do tópico de onde a publicação chegou.
     * @param mqttMsg Envelope MQTT contendo o payload em bytes UTF-8.
     */
    @Override
    public void messageArrived(String fromTopic, MqttMessage mqttMsg) {
        String json = new String(mqttMsg.getPayload(), StandardCharsets.UTF_8);
        Message msg;
        try {
            msg = gson.fromJson(json, Message.class);
        } catch (RuntimeException e) {
            log("payload inválido ignorado: " + json);
            return;
        }
        synchronized (this) {
            if (buffering) {
                buffer.add(msg);
                System.out.printf("[S%d][JOIN] seq=%d guardada no buffer%n", id, msg.seq);
                return;
            }
        }
        if (store.add(msg)) {
            log("recebido seq=" + msg.seq + " \"" + msg.text + "\" (total=" + store.size() + ")");
        } else {
            log("duplicata ignorada seq=" + msg.seq);
        }
    }

    /**
     * Notificado pelo Paho quando a conexão com o broker é estabelecida ou restabelecida.
     *
     * Quando ocorre reconexão ({@code reconnect == true}) e este nó é assinante, dispara a
     * reinscrição no tópico em uma thread separada, já que {@code cleanSession=true} faz o broker
     * descartar a assinatura anterior.
     *
     * @param reconnect {@code true} se foi uma reconexão automática após queda; {@code false} na conexão inicial.
     * @param serverUri URI do broker MQTT ao qual o cliente se conectou.
     */
    @Override
    public void connectComplete(boolean reconnect, String serverUri) {
        if (reconnect && subscriber) {
            log("reconectado ao broker " + serverUri);
            new Thread(() -> {
                try {
                    subscribe();
                } catch (MqttException e) {
                    log("falha ao reinscrever: " + e.getMessage());
                }
            }, "mqtt-resubscribe-" + id).start();
        }
    }

    /**
     * Notificado pelo Paho quando a conexão com o broker MQTT cai.
     *
     * @param cause Exceção que descreve o motivo da perda de conexão.
     */
    @Override
    public void connectionLost(Throwable cause) {
        log("conexão com o broker perdida: " + cause.getMessage());
    }

    /**
     * Notificado pelo Paho quando a entrega de uma publicação ao broker é concluída.
     *
     * @param token Token de entrega associado à mensagem publicada.
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    /**
     * Imprime uma mensagem de log da área {@code MQTT} para este servidor.
     *
     * @param text Mensagem a registrar na saída padrão.
     */
    private void log(String text) {
        System.out.printf("[S%d][MQTT] %s%n", id, text);
    }
}
