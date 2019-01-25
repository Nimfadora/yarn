package com.vasileva.twitter.consumer;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Converts Kafka offsets from Kafka offset format and vice versa.
 */
class OffsetConverter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parses Kafka offset format string ("{'topic1':{'0':50,'1':-1},'topic2':{'0':-1}}") to
     * convenient format.
     * @param jsonString Kafka offset format string
     * @return offsets by partitions by topic
     * @throws IllegalArgumentException when more than single topic is present in offsetString
     */
    static Offset parseFromJSONString(String jsonString) {
        try {
            Offset offset = MAPPER.readValue(jsonString, Offset.class);
            Preconditions.checkArgument(offset.topic2partitions.size() == 1,
                    "Invalid number of topics in initial offset, should be 1, but was: " + offset.topic2partitions.size());
            return offset;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid offset format, should be: {'topic':{'0':0,'1':'0'}}, ex: " + e.getMessage());
        }
    }

    /**
     * Returns end offset based on the start offset and partition batch size.
     * @param startOffsetString start offset
     * @param partitionBatchSize partition batch size
     * @return end offset
     */
    static String getEndOffset(String startOffsetString, long partitionBatchSize) {
        Offset offset = parseFromJSONString(startOffsetString);
        String topic = offset.topic2partitions.keySet().iterator().next();
        for (Map.Entry<String, Long> part2offset : offset.topic2partitions.get(topic).entrySet()) {
            part2offset.setValue(part2offset.getValue() + partitionBatchSize);
        }
        return convertToJSONString(offset);
    }

    /**
     * Converts Offset to Kafka offset format JSON string ("{'topic1':{'0':50,'1':-1},'topic2':{'0':-1}}").
     * @param offset Offset to be converted
     * @return Kafka offset format JSON strin
     * @throws IllegalStateException when exception occurs while conversion
     */
    private static String convertToJSONString(Offset offset) {
        try {
            return MAPPER.writeValueAsString(offset);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot convert offset to JSON: " + e.getMessage());
        }
    }

    /**
     * Java projection of offset JSON: "{'topic1':{'0':50,'1':-1},'topic2':{'0':-1}}"
     */
    class Offset {
        Map<String, Map<String, Long>> topic2partitions;
    }
}
