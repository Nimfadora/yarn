package com.vasileva.twitter.consumer;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.Serializable;
import java.util.Map;

/**
 * Converts Kafka offsets from Kafka offset format and vice versa.
 */
class OffsetUtils implements Serializable {
    private static final Gson MAPPER = new Gson();

    /**
     * Returns end offset based on the start offset and partition batch size.
     *
     * @param startOffsetString  start offset
     * @param partitionBatchSize partition batch size
     * @return end offset
     */
    static String getEndOffsets(String startOffsetString, long partitionBatchSize) {
        Map<String, Map<String, Long>> topic2partitions = parseTopicPartitionsOffsets(startOffsetString);
        String topic = topic2partitions.keySet().iterator().next();
        for (Map.Entry<String, Long> part2offset : topic2partitions.get(topic).entrySet()) {
            part2offset.setValue(part2offset.getValue() + partitionBatchSize);
        }
        return convertToJSONString(topic2partitions);
    }

    /**
     * Parses Kafka offset format string ("{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}") to
     * convenient format.
     *
     * @param jsonString Kafka offset format string
     * @return offsets by partitions by topic
     * @throws IllegalArgumentException when more than single topic is present in offsetString
     */
    static Map<String, Map<String, Long>> parseTopicPartitionsOffsets(String jsonString) {
        Preconditions.checkArgument(jsonString != null, "Offset string must not be null");
        Preconditions.checkArgument(!jsonString.isEmpty(), "Offset string must not be empty");

        try {
            Map<String, Map<String, Long>> topics2partitions = MAPPER.fromJson(jsonString,
                    new TypeToken<Map<String, Map<String, Long>>>() {}.getType());
            Preconditions.checkArgument(topics2partitions.size() == 1,
                    "Invalid number of topics in initial offset, should be 1, but was: " + topics2partitions.size());
            return topics2partitions;
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Invalid offset format: can not be parsed");
        }
    }

    /**
     * Converts Offset to Kafka offset format JSON string ("{'topic1':{'0':50,'1':-1},'topic2':{'0':-1}}").
     *
     * @param topic2PartitionsOffsets topic offsets to be converted
     * @return Kafka offset format JSON string
     * @throws IllegalStateException when exception occurs while conversion
     */
    static String convertToJSONString(Map<String, Map<String, Long>> topic2PartitionsOffsets) {
            return MAPPER.toJson(topic2PartitionsOffsets);

    }
}
