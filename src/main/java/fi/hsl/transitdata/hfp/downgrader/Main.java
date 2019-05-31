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

        MqttConnector connector = null;

        try {
            final Config config = ConfigParser.createConfig();
            final Optional<Credentials> credentials = Credentials.readMqttCredentials(config, "mqtt-broker-in");

            log.info("Configurations read, connecting.");

            connector = new MqttConnector(config, "mqtt-broker-in", credentials);

            final MessageProcessor processor = new MessageProcessor(config, connector);
            //Let's subscribe to connector before connecting so we'll get all the events.
            connector.subscribe(processor);

            connector.connect();

            log.info("Connections established, let's process some messages");
        }
        catch (Exception e) {
            log.error("Exception at main", e);
            if (connector != null) {
                connector.close();
            }
        }
    }
}
