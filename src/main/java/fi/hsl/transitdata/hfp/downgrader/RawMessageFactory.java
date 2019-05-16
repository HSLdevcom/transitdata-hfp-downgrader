package fi.hsl.transitdata.hfp.downgrader;

import fi.hsl.common.hfp.HfpJson;
import fi.hsl.common.hfp.HfpParser;
import fi.hsl.common.mqtt.proto.Mqtt;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class RawMessageFactory implements IMapperFactory {
    private static final Logger log = LoggerFactory.getLogger(RawMessageFactory.class);

    private final static Map<String, String> properties;
    private final HfpParser parser = HfpParser.newInstance();
    static {
        HashMap<String, String> props = new HashMap<>();
        props.put(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.MqttRawMessage.toString());
        props.put(TransitdataProperties.KEY_SCHEMA_VERSION, Integer.toString(Mqtt.RawMessage.newBuilder().getSchemaVersion()));
        properties = Collections.unmodifiableMap(props);
    }

    @Override
    public BiFunction<String, byte[], byte[]> createMapper() {
        return (topic, payload) -> {
            try {
                final HfpJson json = parser.parseJson(payload);
                final HfpJson.Payload data = json.VP;
                final Instant instant = Instant.parse(data.tst).truncatedTo(ChronoUnit.SECONDS);
                data.tst = DateTimeFormatter.ISO_INSTANT.format(instant);
                return parser.serializeToByteArray(json);
            } catch (final IOException e) {
                log.error("Failed to parse HFP data to JSON", e);
            }
            return null;
        };
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }
}
