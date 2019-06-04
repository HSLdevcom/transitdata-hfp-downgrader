package fi.hsl.transitdata.hfp.downgrader;

import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void handleMessage(final Mqtt3Publish message) throws Exception {
        try {
            if (!connectorIn.isConnected()) {
                throw new Exception("MQTT client (in) is no longer connected");
            }
            if (!connectorOut.isConnected()) {
                throw new Exception("MQTT client (out) is no longer connected");
            }

            byte[] convertedPayload = null;
            if (mapper != null) {
                convertedPayload = mapper.apply(null, message.getPayloadAsBytes());
            }

            if (convertedPayload != null) {
                final String downgradedTopic = downgradeTopic(message.getTopic());
                connectorOut.publish(downgradedTopic, convertedPayload)
                        .whenComplete(((mqtt3Publish, throwable) -> {
                            if (throwable != null) {
                                //
                            } else {
                                inFlightCounter.decrementAndGet();
                            }
                        }));
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
            else {
                log.warn("Cannot forward message because converted payload is null");
            }

        }
        catch (Exception e) {
            log.error("Error while handling message", e);
            close(true);
            throw e;
        }

    }

    public static String downgradeTopic(final MqttTopic topic) throws Exception {
        final String topicStr = topic.toString();
        final Matcher matcher = topicPattern.matcher(topicStr);
        String convertedTopic;
        if (matcher.matches() && matcher.groupCount() == 2) {
            final String middle = matcher.group(1);
            final String rest = matcher.group(2);
            StringBuilder builder = new StringBuilder();
            builder.append("/hfp/v1");
            builder.append(middle);
            builder.append(rest);
            convertedTopic = builder.toString();
        } else {
            throw new Exception(String.format("Failed to parse topic %s", topicStr));
        }
        return convertedTopic;
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
        System.exit(0);
    }
}
