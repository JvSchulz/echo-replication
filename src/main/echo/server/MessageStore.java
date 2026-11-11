package echo.server;

import echo.common.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Histórico local de mensagens de uma réplica. Todos os métodos são {@code synchronized}
 * porque chamadas RMI e notificações MQTT chegam em threads diferentes.
 */
public class MessageStore {
    private final List<Message> messages = new ArrayList<>();
    private final Set<UUID> seenIds = new HashSet<>();
    private long nextSeq = 1;

    /**
     * Cria uma nova mensagem com número de sequência monotônico e a grava em um único passo atômico.
     *
     * Atribuir {@code seq} e inserir na lista sob o mesmo lock garante que a ordem da lista
     * é sempre idêntica à ordem de {@code seq}.
     *
     * @param originId Identificador do servidor mestre que está criando a mensagem.
     * @param text Texto enviado pelo cliente.
     * @return Instância de {@link Message} recém-criada e já adicionada ao histórico local.
     */
    public synchronized Message appendNew(int originId, String text) {
        Message msg = new Message(UUID.randomUUID(), nextSeq++, originId, text, System.currentTimeMillis());
        messages.add(msg);
        seenIds.add(msg.msgId);
        return msg;
    }

    /**
     * Insere no histórico uma mensagem recebida por replicação MQTT ou transferência de estado,
     * descartando duplicatas pelo {@code msgId}.
     *
     * A entrega com QoS 1 pode repetir mensagens; por isso, um {@code msgId} já presente em
     * {@code seenIds} é ignorado. Também avança {@code nextSeq} para {@code max(seq) + 1}
     * caso esta réplica seja promovida a mestre no futuro.
     *
     * @param msg Mensagem recebida do broker MQTT ou copiada do mestre via RMI.
     * @return {@code true} se a mensagem era inédita e foi armazenada; {@code false} se era duplicata.
     */
    public synchronized boolean add(Message msg) {
        if (!seenIds.add(msg.msgId)) {
            return false;
        }
        messages.add(msg);
        nextSeq = Math.max(nextSeq, msg.seq + 1);
        return true;
    }

    /**
     * Mescla um lote de mensagens ao histórico local sem duplicatas e reordena a lista por {@code seq}.
     *
     * Usado na entrada de uma réplica para combinar o histórico copiado do mestre via
     * {@code getState()} com as notificações acumuladas no buffer durante a cópia.
     *
     * @param incoming Coleção de mensagens a incorporar ao histórico.
     */
    public synchronized void addAll(Collection<Message> incoming) {
        for (Message msg : incoming) {
            add(msg);
        }
        messages.sort(Comparator.comparingLong(m -> m.seq));
    }

    /**
     * Retorna uma cópia defensiva da lista completa de mensagens armazenadas.
     *
     * @return Nova lista contendo todas as instâncias de {@link Message} na ordem atual.
     */
    public synchronized List<Message> snapshot() {
        return new ArrayList<>(messages);
    }

    /**
     * Extrai apenas o conteúdo textual de cada mensagem do histórico, mantendo a ordem de sequência.
     *
     * @return Nova lista com os textos das mensagens armazenadas, pronta para {@code getListOfMsg()}.
     */
    public synchronized List<String> texts() {
        List<String> list = new ArrayList<>();
        for (Message msg : messages) {
            list.add(msg.text);
        }
        return list;
    }

    /**
     * Retorna a quantidade total de mensagens únicas armazenadas neste servidor.
     *
     * @return Número de mensagens no histórico local.
     */
    public synchronized int size() {
        return messages.size();
    }

    /**
     * Retorna o próximo número de sequência que este nó atribuiria se fosse (ou se tornasse) mestre.
     *
     * @return Próximo valor de {@code seq} (1 se o histórico estiver vazio, ou {@code max(seq) + 1}).
     */
    public synchronized long nextSeq() {
        return nextSeq;
    }
}
