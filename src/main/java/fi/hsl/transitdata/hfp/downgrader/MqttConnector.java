package fi.hsl.transitdata.hfp.downgrader;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3ClientBuilder;
import com.hivemq.client.mqtt.mqtt3.message.connect.Mqtt3ConnectBuilder;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class MqttConnector {
    private static final Logger log = LoggerFactory.getLogger(MqttConnector.class);

    public String mqttTopic;
    public final int qos;
    public final String clientId;
    public final String broker;
    public final int port;
    public final boolean retainMessage;
    public final boolean cleanSession;
    public final int keepAlive;
    private final Optional<Credentials> maybeCredentials;

    private final LinkedList<IMqttMessageHandler> handlers = new LinkedList<>();

    private Mqtt3AsyncClient client;

    public MqttConnector(final Config config, final String mqttConfigRoot, final Optional<Credentials> maybeCredentials) {
        try {
            mqttTopic = config.getString(mqttConfigRoot + ".topic");
        } catch (ConfigException.Missing e) {
            log.warn("Topic config is missing {}", mqttConfigRoot + ".topic");
        }
        qos = config.getInt(mqttConfigRoot + ".qos");
        clientId = createClientId(config, mqttConfigRoot);
        broker = config.getString(mqttConfigRoot + ".host");
        port = config.getInt(mqttConfigRoot + ".port");
        retainMessage = config.getBoolean(mqttConfigRoot + ".retainMessage");

        final int maxInFlight = config.getInt(mqttConfigRoot + ".maxInflight");
        cleanSession = config.getBoolean(mqttConfigRoot + ".cleanSession");
        keepAlive = 60;

        this.maybeCredentials = maybeCredentials;
    }

    private static String createClientId(final Config config, final String mqttConfigRoot) {
        String clientId = config.getString(mqttConfigRoot + ".clientId");
        if (config.getBoolean(mqttConfigRoot + ".addRandomnessToClientId")) {
            clientId += "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        return clientId;
    }

    public void addHandler(IMqttMessageHandler handler) {
        //let's not subscribe to the actual client. we have our own observables here
        //since we want to provide the disconnected-event via the same interface.
        handlers.add(handler);
    }

    public CompletableFuture<Mqtt3ConnAck> connect() throws Exception {
            final Mqtt3ClientBuilder builder = MqttClient.builder()
                    .useMqttVersion3()
                    .identifier(clientId)
                    .serverHost(broker)
                    .serverPort(port);

            if (port == 8883) {
                builder.useSslWithDefaultConfig();
            }

            client = builder.buildAsync();

            Mqtt3ConnectBuilder.Send<CompletableFuture<Mqtt3ConnAck>> connection = client.connectWith()
                    .cleanSession(cleanSession)
                    .keepAlive(keepAlive);

            maybeCredentials.map(credentials -> {
                connection.simpleAuth()
                        .username(credentials.username)
                        .password(credentials.password.getBytes())
                        .applySimpleAuth();
                return connection;
            });

            return connection.send()
                .whenComplete(((mqtt3ConnAck, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to connect to MQTT broker {}", broker);
                        close();
                        throw new RuntimeException(throwable);
                    } else {
                        log.info("Connected to MQTT broker {}", broker);
                    }
                }));
    }

    public CompletableFuture<Mqtt3SubAck> subscribe() {
        return client.subscribeWith()
                .topicFilter(mqttTopic)
                .qos(MqttQos.AT_MOST_ONCE)
                .callback(mqtt3Publish -> {
                    try {
                        messageArrived(mqtt3Publish);
                    } catch (Exception e) {
                        close();
                        throw new RuntimeException(e);
                    }
                })
                .send()
                .whenComplete(((mqtt3SubAck, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to subscribe to topic {}", mqttTopic);
                        close();
                        throw new RuntimeException(throwable);
                    } else {
                        log.info("Subscribed to topic {}", mqttTopic);
                    }
                }));
    }

    public CompletableFuture<Mqtt3Publish> publish(final String topic, final byte[] payload) {
        return client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_MOST_ONCE)
                .retain(retainMessage)
                .payload(payload)
                .send()
                .whenComplete(((mqtt3Publish, throwable) -> {
                    if (throwable != null) {
                        log.warn("Failed to publish {}", topic);
                    } else {
                        //deliveryComplete();
                    }
                }));
    }

    private void messageArrived(final Mqtt3Publish mqtt3Publish) throws Exception {
        for (IMqttMessageHandler handler: handlers) {
            handler.handleMessage(mqtt3Publish);
        }
    }

    private void deliveryComplete() {}

    public boolean isConnected() {
        if (client != null) {
            return client.getConfig().getState().isConnected();
        }
        return false;
    }

    public void close() {
        if (client != null) {
            client.disconnect();
        }
    }
}
