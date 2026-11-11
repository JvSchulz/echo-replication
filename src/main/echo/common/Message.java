package echo.common;

import java.io.Serializable;
import java.util.UUID;

/**
 * Mensagem imutável do histórico replicado. Trafega por RMI ({@link Serializable},
 * em {@code getState()}) e por MQTT (serializada em JSON no tópico de replicação).
 */
public final class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    public final UUID msgId;
    public final long seq;
    public final int originId;
    public final String text;
    public final long timestamp;

    /**
     * Constrói uma mensagem imutável do histórico replicado.
     *
     * @param msgId Identificador único global (UUID) usado para deduplicação nas réplicas.
     * @param seq Número de sequência monotônico atribuído pelo mestre para ordem total.
     * @param originId Identificador do servidor mestre que recebeu a chamada original do cliente.
     * @param text Conteúdo textual enviado pelo cliente.
     * @param timestamp Instante de criação da mensagem no mestre, em milissegundos desde a época Unix.
     */
    public Message(UUID msgId, long seq, int originId, String text, long timestamp) {
        this.msgId = msgId;
        this.seq = seq;
        this.originId = originId;
        this.text = text;
        this.timestamp = timestamp;
    }
}
