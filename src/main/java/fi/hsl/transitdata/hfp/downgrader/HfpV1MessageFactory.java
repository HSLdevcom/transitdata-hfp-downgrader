package fi.hsl.transitdata.hfp.downgrader;

import fi.hsl.common.hfp.HfpJson;
import fi.hsl.common.hfp.HfpParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.BiFunction;

public class HfpV1MessageFactory implements IMapperFactory {
    private static final Logger log = LoggerFactory.getLogger(HfpV1MessageFactory.class);

    private final HfpParser parser = HfpParser.newInstance();

    @Override
    public BiFunction<String, byte[], byte[]> createMapper() {
        return (topic, payload) -> {
            try {
                final HfpJson json = parser.parseJson(payload);
                final HfpJson.Payload data = json.VP;
                final Instant instant = Instant.parse(data.tst).truncatedTo(ChronoUnit.SECONDS);
                data.tst = DateTimeFormatter.ISO_INSTANT.format(instant);
                return parser.serializeToByteArray(json);
            } catch (final Exception e) {
                log.error("Failed to parse HFP data to JSON", e);
            }
            return null;
        };
    }

    @Override
    public Map<String, String> properties() {
        return null;
    }
}
