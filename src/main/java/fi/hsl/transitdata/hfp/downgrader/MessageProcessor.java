package fi.hsl.transitdata.hfp.downgrader;

import com.typesafe.config.Config;
import fi.hsl.common.hfp.HfpParser;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.BiFunction;

public class MessageProcessor implements IMqttMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    final MqttConnector connectorIn;
    final MqttConnector connectorOut;

    private boolean shutdownInProgress = false;

    private final int IN_FLIGHT_ALERT_THRESHOLD;
    private final int MSG_MONITORING_INTERVAL;

    private final BiFunction<String, byte[], byte[]> mapper;

    public MessageProcessor(Config config, MqttConnector connectorIn, MqttConnector connectorOut) {
        this.connectorIn = connectorIn;
        this.connectorOut = connectorOut;

        IN_FLIGHT_ALERT_THRESHOLD = config.getInt("application.inFlightAlertThreshold");
        MSG_MONITORING_INTERVAL = config.getInt("application.msgMonitoringInterval");
        log.info("Using in-flight alert threshold of {} with monitoring interval of {} messages", IN_FLIGHT_ALERT_THRESHOLD, MSG_MONITORING_INTERVAL);

        IMapperFactory factory = new HfpV1MessageFactory();
        mapper = factory.createMapper();
    }

    @Override
    public void handleMessage(final String topic, final MqttMessage message) throws Exception {
        try {
            if (!connectorIn.client.isConnected()) {
                throw new Exception("MQTT client (in) is no longer connected");
            }
            if (!connectorOut.client.isConnected()) {
                throw new Exception("MQTT client (out) is no longer connected");
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
            throw e;
        }

    }

    public static String downgradeTopic(final String topic) throws Exception {
        final String[] parts = topic.split("/", -1); // -1 to include empty substrings
        final int versionIndex = HfpParser.findVersionIndex(parts);
        if (versionIndex < 0) {
            throw new Exception("Failed to find topic version from topic: " + topic);
        }
        final String versionStr = parts[versionIndex];
        if (!versionStr.equals("v2")) {
            throw new Exception("Topic version is not v2: " + topic);
        }
        parts[versionIndex] = "v1";
        final String[] start = Arrays.copyOfRange(parts, 0, 5);
        final String[] end = Arrays.copyOfRange(parts, 6, parts.length);
        return String.join("/", start) + "/" + String.join("/", end);
    }

    private void publish(final String topic, final byte[] payload) throws Exception {
        try {
            connectorOut.client.publish(topic, payload, connectorOut.qos, connectorOut.retainMessage);
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
            connectorIn.close();
            connectorOut.close();
            log.info("MQTT connection closed");
        }

        log.info("Pulsar connection closed");
    }
}
