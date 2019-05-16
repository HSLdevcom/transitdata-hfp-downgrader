package fi.hsl.transitdata.hfp.downgrader;

import com.typesafe.config.Config;
import fi.hsl.common.hfp.HfpParser;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final MqttConnector connector;

    private boolean shutdownInProgress = false;
    private final AtomicInteger inFlightCounter = new AtomicInteger(0);
    private int msgCounter = 0;

    private final int IN_FLIGHT_ALERT_THRESHOLD;
    private final int MSG_MONITORING_INTERVAL;

    private final BiFunction<String, byte[], byte[]> mapper;
    private final Map<String, String> properties;

    public MessageProcessor(Config config, MqttConnector connector) {
        this.connector = connector;

        IN_FLIGHT_ALERT_THRESHOLD = config.getInt("application.inFlightAlertThreshold");
        MSG_MONITORING_INTERVAL = config.getInt("application.msgMonitoringInterval");
        log.info("Using in-flight alert threshold of {} with monitoring interval of {} messages", IN_FLIGHT_ALERT_THRESHOLD, MSG_MONITORING_INTERVAL);

        IMapperFactory factory = new HfpV1MessageFactory();
        mapper = factory.createMapper();
        properties = factory.properties();
    }

    @Override
    public void handleMessage(final String topic, final MqttMessage message) throws Exception {
        try {
            // This method is invoked synchronously by the MQTT client (via our connector), so all events arrive in the same thread
            // https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttCallback.html

            // Optimally we would like to send the event to Pulsar synchronously and validate that it was a success,
            // and only after that acknowledge the message to mqtt by returning gracefully from this function.
            // This works if the rate of incoming messages is low enough to complete the Pulsar transaction.
            // This would allow us to deliver messages once and only once, in insertion order

            // If we want to improve our performance and lose guarantees of once-and-only-once,
            // we can optimize the pipeline by sending messages asynchronously to Pulsar.
            // This means that data will be lost on insertion errors.
            // Using a single producer however should guarantee insertion-order guarantee between two consecutive messages.

            // Current implementation uses the latter approach

            if (!connector.client.isConnected()) {
                throw new Exception("MQTT client is no longer connected");
            }

            byte[] payload = message.getPayload();
            if (mapper != null) {
                payload = mapper.apply(null, payload);
            }

            if (payload != null) {
                final String downgradedTopic = downgradeTopic(topic);
                publish(downgradedTopic, payload);
            }
            else {
                log.warn("Cannot forward message because (mapped) content is null");
            }

        }
        catch (Exception e) {
            log.error("Error while handling the message", e);
            // Let's close everything and restart.
            // Closing the MQTT connection should enable us to receive the same message again.
            close(true);
        }

    }

    private String downgradeTopic(final String topic) throws Exception {
        final String[] parts = topic.split("/", -1); // -1 to include empty substrings
        final int versionIndex = HfpParser.findVersionIndex(parts);
        if (versionIndex < 0) {
            throw new Exception("Failed to find topic version from topic: " + topic);
        }
        final String versionStr = parts[versionIndex];
        if (!versionStr.equals("v2")) {
            throw new Exception("Topic version is not v2: " + topic);
        }
        if (parts.length != 19) {
            throw new Exception("Topic has unexpected number of parts: " + topic);
        }
        parts[versionIndex] = "v1";
        final String[] start = Arrays.copyOfRange(parts, 0, 5);
        final String[] end = Arrays.copyOfRange(parts, 6, parts.length);
        return String.join("/", start) + "/" + String.join("/", end);
    }

    private void publish(final String topic, final byte[] payload) throws Exception {
        try {
            final MqttMessage message = new MqttMessage(payload);
            message.setQos(connector.qos);
            message.setRetained(connector.retainMessage);
            connector.client.publish(topic, message, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    inFlightCounter.decrementAndGet();
                    log.debug("MQTT message delivered");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    List<String> topics = Arrays.asList(asyncActionToken.getTopics());
                    String msg = "Failed to send message [" + asyncActionToken.getMessageId() + "] to topics " + String.join(", ", topics);
                    log.error(msg, exception);
                }
            });
            int inFlight = inFlightCounter.incrementAndGet();
            if (++msgCounter % MSG_MONITORING_INTERVAL == 0) {
                if (inFlight < 0 || inFlight > IN_FLIGHT_ALERT_THRESHOLD) {
                    log.error("Pulsar insert cannot keep up with the MQTT feed! In flight: {}", inFlight);
                }
                else {
                    log.info("Currently messages in flight: {}", inFlight);
                }
            }
        }
        catch (Exception e) {
            log.error("Error publishing MQTT message", e);
            throw e;
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("Mqtt connection lost");
        close(false);
    }

    public void close(boolean closeMqtt) {
        if (shutdownInProgress) {
            return;
        }
        shutdownInProgress = true;

        log.warn("Closing MessageProcessor resources");
        //Let's first close the MQTT to stop the event stream.
        if (closeMqtt) {
            connector.close();
            log.info("MQTT connection closed");
        }

        log.info("Pulsar connection closed");
    }
}
