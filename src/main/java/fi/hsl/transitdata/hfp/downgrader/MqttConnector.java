package fi.hsl.transitdata.hfp.downgrader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;
import java.util.UUID;

public class MqttConnector implements MqttCallback {
    private static final Logger log = LoggerFactory.getLogger(MqttConnector.class);

    public final String name;
    private String mqttTopic;
    public final int qos;
    private final String clientId;
    private final String broker;
    public final boolean retainMessage;

    private final MqttConnectOptions connectOptions;
    private final LinkedList<IMqttMessageHandler> handlers = new LinkedList<>();

    public MqttAsyncClient client;

    public MqttConnector(final Config config, final String mqttConfigRoot, final Optional<Credentials> maybeCredentials) {
        switch (mqttConfigRoot) {
            case "mqtt-broker-in":
                name = "in";
                break;
            case "mqtt-broker-out":
                name = "out";
                break;
            default:
                throw new IllegalArgumentException("mqttConfigRoot is neither mqtt-broker-in nor mqtt-broker-out");
        }
        try {
            mqttTopic = config.getString(mqttConfigRoot + ".topic");
        } catch (ConfigException.Missing e) {
            log.warn("({}) Topic config is missing {}", name, mqttConfigRoot + ".topic");
        }
        qos = config.getInt(mqttConfigRoot + ".qos");
        clientId = createClientId(config, mqttConfigRoot);
        broker = config.getString(mqttConfigRoot + ".host");
        retainMessage = config.getBoolean(mqttConfigRoot + ".retainMessage");

        final int maxInFlight = config.getInt(mqttConfigRoot + ".maxInflight");
        final boolean cleanSession = config.getBoolean(mqttConfigRoot + ".cleanSession");

        connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(cleanSession);
        connectOptions.setMaxInflight(maxInFlight);
        connectOptions.setAutomaticReconnect(false); //Let's abort on connection errors

        maybeCredentials.ifPresent(credentials -> {
            connectOptions.setUserName(credentials.username);
            connectOptions.setPassword(credentials.password.toCharArray());
        });
        connectOptions.setConnectionTimeout(10);
    }

    String createClientId(final Config config, final String mqttConfigRoot) {
        String clientId = config.getString(mqttConfigRoot + ".clientId");
        if (config.getBoolean(mqttConfigRoot + ".addRandomnessToClientId")) {
            clientId += "-" + UUID.randomUUID().toString().substring(0, 8);
        }
        return clientId;
    }

    public void subscribe(IMqttMessageHandler handler) {
        //let's not subscribe to the actual client. we have our own observables here
        //since we want to provide the disconnected-event via the same interface.
        log.info("({}) Adding subscription", name);
        handlers.add(handler);
    }

    public void connect() throws Exception {
        try {
            //Let's use memory persistance to optimize throughput.
            MemoryPersistence memoryPersistence = new MemoryPersistence();

            client = new MqttAsyncClient(broker, clientId, memoryPersistence);
            client.setCallback(this); //Let's add the callback before connecting so we won't lose any messages

            log.info("({}) Connecting to mqtt broker {}", name, broker);
            final IMqttToken token = client.connect(connectOptions, null, new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log.info("({}) Connected", name);
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log.error(String.format("(%s) Connection failed", name), exception);
                }
            });
            token.waitForCompletion();
            if (token.isComplete()) {
                log.info("({}) Connection to MQTT completed", name);
            } else {
                log.warn("({}) Connection to MQTT is not completed", name);
            }
            if (token.getException() != null) {
                throw token.getException();
            }
        }
        catch (Exception e) {
            log.error(String.format("(%s) Error connecting to MQTT broker", name), e);
            if (client != null) {
                //Paho doesn't close the connection threads unless we force-close it.
                client.close(true);
            }
            throw e;
        }

        if (mqttTopic != null) {
            try {
                log.info("({}) Subscribing to topic {} with QoS {}", name, mqttTopic, qos);
                client.subscribe(mqttTopic, qos);
            } catch (Exception e) {
                log.error(String.format("(%s) Error subscribing to MQTT broker", name), e);
                close();
                throw e;
            }
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error(String.format("(%s) Connection to mqtt broker lost, notifying clients", name), cause);
        for (IMqttMessageHandler handler: handlers) {
            handler.connectionLost(cause);
        }
        close();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        for (IMqttMessageHandler handler: handlers) {
            handler.handleMessage(topic, message);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

    public void close() {
        if (client == null) {
            log.warn("({}) Cannot close mqtt connection since it's null", name);
            return;
        }
        try {
            log.info("({}) Closing MqttConnector resources", name);
            //Paho doesn't close the connection threads unless we first disconnect and then force-close it.
            client.disconnectForcibly(1000L, 1000L);
            client.close(true);
            client = null;
        }
        catch (Exception e) {
            log.error(String.format("(%s) Failed to close MQTT client connection", name), e);
        }
    }
}
