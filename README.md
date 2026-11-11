# Serviço de Echo com Replicação Passiva e Eleição em Anel

Implementação em Java de um serviço distribuído de echo de mensagens com **replicação passiva** (mestre/réplicas), invocação remota cliente -> mestre via **Java RMI**, disseminação de estado mestre -> réplicas via **MQTT** (publish/subscribe no broker Mosquitto) e **eleição distribuída em anel** (algoritmo de Chang-Roberts) com detecção de falhas por *heartbeat*.

## Estrutura dos Arquivos Fonte

```text
src/main/echo/
├── common/
│   ├── EchoService.java      # Interface RMI cliente -> mestre (echo, getListOfMsg)
│   ├── ReplicaControl.java   # Interface RMI servidor <-> servidor (ping, election, elected, getState)
│   └── Message.java          # Mensagem serializável (msgId, seq, originId, text, timestamp)
├── server/
│   ├── ServerMain.java       # Ponto de entrada do servidor (--id, --registry, --broker, --topic)
│   ├── ReplicaServer.java    # Objeto remoto RMI: coordena join, replicação, heartbeat e promoção
│   ├── MessageStore.java     # Histórico local thread-safe com deduplicação por msgId
│   ├── MqttReplicator.java   # Publicação (QoS 1), assinatura, reconexão e buffer de entrada
│   ├── FailureDetector.java  # Monitoramento periódico do mestre via ping() RMI
│   └── RingElection.java     # Eleição em anel (Chang-Roberts, maior ID ativo vence)
└── client/
    ├── ClientMain.java       # CLI interativa do cliente (echo <texto>, list, quit)
    └── EchoClient.java       # Cliente RMI com reconexão automática e backoff exponencial
```

## Pré-requisitos e Bibliotecas Utilizadas

- **JDK 11** ou superior (`sudo apt install default-jdk`).
- **Mosquitto** (broker MQTT) e clientes de linha de comando:
  ```bash
  sudo apt install mosquitto mosquitto-clients
  ```
- **Bibliotecas externas utilizadas**:
  - **Eclipse Paho MQTT v3** (`org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5`) - cliente MQTT para publicação e assinatura no broker.
  - **Google Gson** (`com.google.code.gson:gson:2.15.0`) - serialização e desserialização das mensagens em JSON.

---

## Como Criar e Configurar o Projeto no IntelliJ IDEA (Ubuntu)

Você pode criar um projeto novo no IntelliJ IDEA a partir dos arquivos fontes ou abrir diretamente via Maven.

### Opção A - Criando um projeto novo no IntelliJ IDEA a partir dos fontes

1. Abra o IntelliJ IDEA e selecione **New Project**.
2. Em **Name**, informe `echo-replication`, selecione **Language: Java**, **Build system: IntelliJ** (ou **Maven**), **JDK: 11** (ou superior) e clique em **Create**.
3. Copie a pasta `echo/` (contendo `common/`, `server/` e `client/`) para dentro do diretório `src/` do projeto criado (de modo que os pacotes fiquem em `src/echo/common`, `src/echo/server` e `src/echo/client`).
   - Caso mantenha a estrutura `src/main/echo/`, clique com o botão direito na pasta `src/main` na árvore do projeto e selecione **Mark Directory as > Sources Root**.
4. **Adicione as bibliotecas utilizadas (Paho MQTT e Gson)**:
   - Abra **File > Project Structure > Libraries > + > From Maven...** (ou selecione **Java** para apontar para arquivos `.jar` locais).
   - Adicione as duas dependências:
     - `org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5`
     - `com.google.code.gson:gson:2.15.0`
   - Clique em **Apply** e **OK**.
5. **Configure as execuções (Run Configurations)**:
   - Vá em **Run > Edit Configurations > + > Application**:
     - **Servidor**: *Name* = `Server 1`, *Main class* = `echo.server.ServerMain`, *Program arguments* = `--id 1` (marque *Modify options > Allow multiple instances* para subir várias réplicas mudando `--id 2`, `--id 3`, etc.).
     - **Cliente**: *Name* = `Client`, *Main class* = `echo.client.ClientMain`.

### Opção B - Abrindo diretamente com o `pom.xml` (Maven)

1. No IntelliJ IDEA, acesse **File > Open...**, selecione a pasta raiz deste projeto (onde está o `pom.xml`) e clique em **Open as Project**.
2. O IntelliJ reconhecerá automaticamente o diretório de fontes (`src/main`) e baixará as bibliotecas **Eclipse Paho (`1.2.5`)** e **Gson (`2.15.0`)** declaradas no `pom.xml`.
3. Em **File > Project Structure > Project**, confira se o **SDK** está definido como **11** (ou superior).
4. Para gerar o pacote executável pelo painel lateral **Maven**, execute **Lifecycle > package** (ou rode `./mvnw package` no terminal).

---

## Compilação pelo Terminal

```bash
./mvnw package
```

O comando acima baixa o Maven e as bibliotecas automaticamente e gera o pacote `target/echo.jar` (com Paho e Gson embutidos) e as classes compiladas em `target/classes`.

---

## Como Executar (Ordem de Inicialização)

Abra um terminal na raiz do projeto para cada componente, seguindo a ordem **broker -> serviço de nomes -> servidores -> clientes**:

1. **Iniciar o broker MQTT (Mosquitto)**:
   ```bash
   sudo systemctl start mosquitto          # ou: mosquitto -v
   ```
2. **(Opcional) Observar o tópico MQTT em tempo real**:
   ```bash
   mosquitto_sub -t echo/replication -v
   ```
3. **Iniciar o serviço de nomes (`rmiregistry`)**:
   ```bash
   scripts/start-registry.sh               # equivalente a: rmiregistry -J-Djava.class.path=target/classes 1099
   ```
4. **Iniciar as réplicas do servidor** (cada uma em um terminal):
   ```bash
   scripts/start-server.sh 1               # ou: java -cp target/echo.jar echo.server.ServerMain --id 1
   scripts/start-server.sh 2               # ou: java -cp target/echo.jar echo.server.ServerMain --id 2
   scripts/start-server.sh 3               # ou: java -cp target/echo.jar echo.server.ServerMain --id 3
   ```
5. **Iniciar o cliente**:
   ```bash
   scripts/start-client.sh                 # ou: java -cp target/echo.jar echo.client.ClientMain
   ```

Para informar endereços customizados:
```bash
java -cp target/echo.jar echo.server.ServerMain --id 1 --registry localhost:1099 --broker tcp://localhost:1883 --topic echo/replication
java -cp target/echo.jar echo.client.ClientMain --registry localhost:1099
```

**Comandos disponíveis no cliente**:
- `echo <texto>` - envia uma mensagem ao mestre e exibe a resposta ecoada.
- `list` - exibe o histórico completo de mensagens armazenadas no serviço.
- `quit` - encerra o cliente.

---

## Roteiro de Teste e Demonstração de Falha

1. Inicie `S1`, `S2`, `S3` e o cliente. `S1` sobe primeiro e se elege mestre; `S2` e `S3` entram como réplicas sincronizadas e passam a monitorar `S1` via *heartbeat*.
2. No cliente, envie `echo msg1`, `echo msg2`, `echo msg3`. Verifique no `mosquitto_sub` os payloads JSON publicados e nos terminais de `S2` e `S3` o recebimento de cada número de sequência (`seq`).
3. **Derrube o mestre atual (`Ctrl+C` no terminal de `S1`)**. Após 3 falhas consecutivas de `ping()` (~3 s), `S2` e `S3` detectam a queda e iniciam a eleição em anel. `S3` (maior ID ativo) vence, faz `unsubscribe` do tópico MQTT e assume o registro `EchoMaster`.
4. No cliente, envie `echo msg4` e depois `list`. O cliente exibe `reconectando ao serviço...`, localiza automaticamente o novo mestre (`S3`) e lista todas as 4 mensagens na ordem correta.
5. Inicie um novo servidor (`scripts/start-server.sh 4`). `S4` se inscreve no tópico (bufferizando novas publicações), copia o histórico acumulado de `S3` via RMI (`getState()`) e passa a integrar o anel.

## Limitações Conhecidas

- **Detector de falhas baseado em tempo**: em um sistema assíncrono, um mestre excessivamente lento pode estourar os timeouts de *heartbeat* e ser considerado inativo pelas réplicas.
- **Janela entre gravação local e publicação MQTT**: se o mestre falhar imediatamente após gravar a mensagem em memória e antes de concluir o `publish` no broker, a mensagem não terá sido enviada às réplicas.
- **Repetição em falha durante resposta**: se o mestre processar e replicar um `echo`, mas cair antes de devolver a confirmação RMI ao cliente, o failover do cliente reenviará a requisição ao novo mestre, gerando uma nova entrada.
- **Armazenamento em memória**: o histórico é mantido exclusivamente em RAM enquanto houver pelo menos uma réplica ativa.
