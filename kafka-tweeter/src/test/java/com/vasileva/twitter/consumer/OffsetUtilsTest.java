package com.vasileva.twitter.consumer;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OffsetUtilsTest {

    @Test
    public void testParseTopicPartitionsOffsets() {
        String jsonString = "{\"spark-twitter\":{\"0\":0,\"1\":0,\"2\":0}}";

        Map<String, Map<String, Long>> expected = ImmutableMap.of("spark-twitter",
                ImmutableMap.of("0", 0L, "1", 0L, "2", 0L));
        Map<String, Map<String, Long>> actual = OffsetUtils.parseTopicPartitionsOffsets(jsonString);

        assertEquals(1, actual.size());
        assertTrue(expected.entrySet().containsAll(actual.entrySet()));
    }

    @Test
    public void testConvertToJSONString() {
        Map<String, Map<String, Long>> topicPartitionsOffsets = ImmutableMap.of("spark-twitter",
                ImmutableMap.of("0", 0L, "1", 0L, "2", 0L));

        String expected = "{\"spark-twitter\":{\"0\":0,\"1\":0,\"2\":0}}";
        String actual = OffsetUtils.convertToJSONString(topicPartitionsOffsets);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetEndOffsets() {
        String startOffsets = "{\"spark-twitter\":{\"0\":0,\"1\":0,\"2\":0}}";

        String expected = "{\"spark-twitter\":{\"0\":6,\"1\":6,\"2\":6}}";
        String actual = OffsetUtils.getEndOffsets(startOffsets, 6);

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeveralTopics() {
        OffsetUtils.parseTopicPartitionsOffsets("{\"spark-twitter\":{\"0\":0,\"1\":0,\"2\":0}, \"spark-twitter-1\":{\"0\":0}}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullOffsets() {
        OffsetUtils.parseTopicPartitionsOffsets(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyOffsets() {
        OffsetUtils.parseTopicPartitionsOffsets("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidOffsetsFormat() {
        OffsetUtils.parseTopicPartitionsOffsets("abra-kadabra");
    }

}
