package fi.hsl.transitdata.hfp.downgrader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Scanner;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class HfpV1MessageFactoryTest {

    @Test
    public void testHfpV1Payload() throws Exception {
        final String content = readJsonFile("hfp-v1-sample.json");
        final byte[] payload = content.getBytes("UTF-8");
        assertNotNull(payload);

        final BiFunction<String, byte[], byte[]> mapper = new HfpV1MessageFactory().createMapper();
        final byte[] mapped = mapper.apply(null, payload);

        assertNotNull(mapped);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode originalJson = objectMapper.readTree(payload);
        JsonNode mappedJson = objectMapper.readTree(mapped);

        assertTrue(originalJson.equals(mappedJson));
    }

    @Test
    public void testHfpV2Payload() throws Exception {
        final String content = readJsonFile("hfp-v2-sample.json");
        final byte[] payload = content.getBytes("UTF-8");
        assertNotNull(payload);

        final BiFunction<String, byte[], byte[]> mapper = new HfpV1MessageFactory().createMapper();
        final byte[] mapped = mapper.apply(null, payload);

        assertNotNull(mapped);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode originalJson = objectMapper.readTree(payload);
        JsonNode mappedJson = objectMapper.readTree(mapped);

        assertFalse(originalJson.equals(mappedJson));
    }

    private String readJsonFile(final String filename) throws IOException {
        final ClassLoader classLoader = getClass().getClassLoader();
        final URL url = classLoader.getResource(filename);
        return new Scanner(url.openStream(), "UTF-8").useDelimiter("\\A").next();
    }
}
