package fi.hsl.transitdata.hfp.downgrader;

import com.typesafe.config.Config;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageProcessor implements IMqttMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private static final Pattern topicPattern = Pattern.compile("^\\/hfp\\/v2(\\/\\w*\\/\\w*)\\/vp(.*)$");

    final MqttConnector connectorIn;
    final MqttConnector connectorOut;

    private boolean shutdownInProgress = false;
    private final AtomicInteger inFlightCounter = new AtomicInteger(0);
    private int msgCounter = 0;

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
                throw new Exception("(in) MQTT client is no longer connected");
            }
            if (!connectorOut.client.isConnected()) {
                throw new Exception("(out) MQTT client is no longer connected");
            }

            final byte[] payload = message.getPayload();

            if (topic != null && payload != null) {
                publish(topic, payload);
            }
            else {
                log.warn("Cannot publish message because payload and/or topic is null");
            }
        }
        catch (Exception e) {
            log.error("Error while handling the message", e);
            // Let's close everything and restart.
            // Closing the MQTT connection should enable us to receive the same message again.
            close();
            throw e;
        }

    }

    public static String downgradeTopic(final String topic) {
        String convertedTopic = null;
        final Matcher matcher = topicPattern.matcher(topic);
        if (matcher.matches() && matcher.groupCount() == 2) {
            final String middle = matcher.group(1);
            final String rest = matcher.group(2);
            final StringBuilder builder = new StringBuilder();
            builder.append("/hfp/v1");
            builder.append(middle);
            builder.append(rest);
            convertedTopic = builder.toString();
        } else {
            log.error("Failed to parse downgrade topic {}", topic);
        }
        return convertedTopic;
    }

    private void publish(final String topic, final byte[] payload) throws Exception {
        try {
            connectorOut.client.publish(topic, payload, connectorOut.qos, connectorOut.retainMessage, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    inFlightCounter.decrementAndGet();
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    final List<String> topics = Arrays.asList(iMqttToken.getTopics());
                    log.error(String.format("Failed to publish message to topics %s"), String.join(", ", topics));
                }
            });
        }
        catch (Exception e) {
            log.error("Error publishing MQTT message", e);
            throw e;
        }
        int inFlight = inFlightCounter.incrementAndGet();
        if (++msgCounter % MSG_MONITORING_INTERVAL == 0) {
            if (inFlight < 0 || inFlight > IN_FLIGHT_ALERT_THRESHOLD) {
                log.error("MQTT client (out) cannot keep up with MQTT client (in)! In flight: {}", inFlight);
            }
            else {
                log.info("Currently messages in flight: {}", inFlight);
            }
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("Mqtt connection lost");
        close();
    }

    public void close() {
        if (shutdownInProgress) {
            return;
        }
        shutdownInProgress = true;

        log.warn("Closing MessageProcessor");
        //Let's first close the MQTT to stop the event stream.
        connectorIn.close();
        connectorOut.close();
        log.info("MQTT connections closed");

        log.info("MessageProcessor closed");
        // Exit forcibly just to make sure
        System.exit(1);
    }
}
