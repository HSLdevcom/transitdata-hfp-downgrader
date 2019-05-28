package fi.hsl.transitdata.hfp.downgrader;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Launching transitdata-hfp-downgrader");

        MqttConnector connectorIn = null;
        MqttConnector connectorOut = null;

        try {
            final Config config = ConfigParser.createConfig();
            final Optional<Credentials> credentialsIn = Credentials.readMqttCredentials(config, "mqtt-broker-in");
            final Optional<Credentials> credentialsOut = Credentials.readMqttCredentials(config, "mqtt-broker-out");

            log.info("Configurations read, connecting.");

            connectorIn = new MqttConnector(config, "mqtt-broker-in", credentialsIn);
            connectorOut = new MqttConnector(config, "mqtt-broker-out", credentialsOut);

            final MessageProcessor processor = new MessageProcessor(config, connectorIn, connectorOut);
            //Let's subscribe to connector before connecting so we'll get all the events.
            connectorIn.subscribe(processor);

            connectorIn.connect();
            connectorOut.connect();

            log.info("Connections established, let's process some messages");
        }
        catch (Exception e) {
            log.error("Exception at main", e);
            if (connectorIn != null) {
                connectorIn.close();
            }
            if (connectorOut != null) {
                connectorOut.close();
            }
        }

    }
}
