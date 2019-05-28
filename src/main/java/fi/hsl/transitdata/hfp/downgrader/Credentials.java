package fi.hsl.transitdata.hfp.downgrader;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;
import java.util.Scanner;

public class Credentials {
    private static final Logger log = LoggerFactory.getLogger(Credentials.class);

    public String username;
    public String password;

    public Credentials(String user, String pw) {
        username = user;
        password = pw;
    }

    public static Optional<Credentials> readMqttCredentials(final Config config, final String mqttConfigRoot) throws Exception {
        try {
            if (!config.getBoolean(mqttConfigRoot + ".credentials.required")) {
                log.info("Login credentials not required");
                return Optional.empty();
            }
            else {
                //Default path is what works with Docker out-of-the-box. Override with a local file if needed
                final String usernamePath = config.getString(mqttConfigRoot + ".credentials.usernameFilepath");
                log.debug("Reading username from " + usernamePath);
                String username = new Scanner(new File(usernamePath)).useDelimiter("\\Z").next();

                final String passwordPath = config.getString(mqttConfigRoot + ".credentials.passwordFilepath");
                log.debug("Reading password from " + passwordPath);
                String password = new Scanner(new File(passwordPath)).useDelimiter("\\Z").next();

                log.info("Login credentials read from files successfully");
                return Optional.of(new Credentials(username, password));
            }
        } catch (Exception e) {
            log.error("Failed to read login credentials from secret files", e);
            throw e;
        }
    }


}
