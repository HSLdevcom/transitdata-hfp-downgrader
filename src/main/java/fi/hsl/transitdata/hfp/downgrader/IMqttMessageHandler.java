package fi.hsl.transitdata.hfp.downgrader;

import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;

/**
 * Paho has multiple interfaces but none of them seem to support our use case of receiving events for both
 * message-arrived and client-disconnected event. So we'll wrap our own interface.
 */
public interface IMqttMessageHandler {
    void handleMessage(final Mqtt3Publish mqtt3Publish) throws Exception;
    void connectionLost(Throwable cause);
}
