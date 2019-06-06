package fi.hsl.transitdata.hfp.downgrader;

import org.junit.Test;

import static org.junit.Assert.*;

public class MessageProcessorTest {
    final static String v1Topic = "/hfp/v1/journey/ongoing/bus/0022/00869/2118B/2/Tapiola (M)/16:23/2241237/5/60;24/17/84/15";
    final static String v2Topic = "/hfp/v2/journey/ongoing/vp/bus/0055/01216/1069/1/Malmi/7:20/1130111/0/60;24/19/73/29";
    final static String downgradedV2Topic = "/hfp/v1/journey/ongoing/bus/0055/01216/1069/1/Malmi/7:20/1130111/0/60;24/19/73/29";

    @Test
    public void downgradeHfpV2Topic() {
        assertEquals(downgradedV2Topic, MessageProcessor.downgradeTopic(v2Topic));
    }

    @Test
    public void downgradeHfpV1Topic() {
        assertNull(MessageProcessor.downgradeTopic(v1Topic));
    }
}
