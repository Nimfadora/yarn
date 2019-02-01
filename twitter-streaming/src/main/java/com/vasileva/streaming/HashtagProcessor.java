package com.vasileva.streaming;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Processes Kafka topics with twits and stats, aggregates stats by hashtags.
 */
public class HashtagProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(HashtagProcessor.class);
    private static final String KEY_DELIMITER = ";";
    private static final Properties KAFKA_PRODUCER_PROPS = new Properties();

    static {
        KAFKA_PRODUCER_PROPS.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        KAFKA_PRODUCER_PROPS.put(ProducerConfig.ACKS_CONFIG, "all");
        KAFKA_PRODUCER_PROPS.put(ProducerConfig.RETRIES_CONFIG, "0");
        KAFKA_PRODUCER_PROPS.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KAFKA_PRODUCER_PROPS.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    }

    /* parsing format: "Thu Jan 24 10:50:50 +0000 2019" */
    private static SimpleDateFormat INPUT_DATE_FMT = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy", Locale.US);
    private static SimpleDateFormat OUTPUT_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd", Locale.US);

    /**
     * Parses date, hour and hashtag from Twitter status, aggregates counts by date, hour and hashtag.
     *
     * @param twits        dStream of userName -> twitterStatus
     * @param windowLength aggregation window length
     * @return aggregated dStream of date;hour;hashtag -> cnt
     */
    public static JavaPairDStream<String, Long> aggTwitHashtags(JavaPairDStream<String, String> twits, Duration windowLength) {
        return twits.flatMapToPair((k2v) -> parseTwit(k2v._2)
                .stream()
                .map(key -> Tuple2.apply(key, 1L))
                .iterator())
                .reduceByKeyAndWindow((cnt1, cnt2) -> cnt1 + cnt2, windowLength, windowLength);
    }

    /**
     * Parses kafka stats record (date;hour -> date;hour;hashtag;cnt) to key2count format (date;hour;hashtag -> cnt),
     * aggregates by key.
     *
     * @param key2entry dStream of date;hour -> date;hour;hashtag;cnt
     * @return aggregated dStream of date;hour;hashtag -> cnt
     */
    public static JavaPairDStream<String, Long> aggStats(JavaPairDStream<String, String> key2entry) {
        return key2entry.mapToPair(k2e -> {
            int cntIdx = k2e._2.lastIndexOf(KEY_DELIMITER) + 1;
            return Tuple2.apply(k2e._2.substring(0, cntIdx - 1), Long.parseLong(k2e._2.substring(cntIdx)));
        }).reduceByKey((v1, v2) -> v1 + v2);
    }

    /**
     * Converts stream of date;hour;hashtag -> cnt to kafka date;hour -> date;hour;hashtag;cnt record format
     *
     * @param key2cnt dStream of date;hour;hashtag -> cnt
     * @return dStream of date;hour -> date;hour;hashtag;cnt
     */
    public static JavaPairDStream<String, String> convertToKafkaMessageStream(JavaPairDStream<String, Long> key2cnt) {
        return key2cnt.mapToPair(dateHourHashtag2cnt -> {
            int hashtagIdx = dateHourHashtag2cnt._1.lastIndexOf(KEY_DELIMITER);
            String entry = String.join(KEY_DELIMITER, dateHourHashtag2cnt._1, String.valueOf(dateHourHashtag2cnt._2));
            return Tuple2.apply(dateHourHashtag2cnt._1.substring(0, hashtagIdx), entry);
        });
    }

    /**
     * Sends records of date;hour -> date;hour;hashtag;cnt format to kafka topic specified
     *
     * @param data  dStream of date;hour -> date;hour;hashtag;cnt format
     * @param topic kafka topic name
     */
    public static void sendToKafka(JavaPairDStream<String, String> data, String topic) {
        data.foreachRDD(rdd -> rdd.foreachPartition(p -> {
            KafkaProducer<String, String> producer = new KafkaProducer<>(KAFKA_PRODUCER_PROPS);
            p.forEachRemaining(key2entry -> producer.send(
                    new ProducerRecord<>(topic, key2entry._1, key2entry._2), callback()));
            producer.close();
        }));
    }

    /**
     * Parses given twitter status, presented in JSON format, extracting date, hour and hashtag.
     *
     * @param twitterStatus JSON twitter status
     * @return stats aggregation key: date;hour;hashtag
     */
    @VisibleForTesting
    public static List<String> parseTwit(String twitterStatus) {
        Gson mapper = new Gson();
        JsonObject statusJSON = mapper.fromJson(twitterStatus, JsonObject.class);

        if (statusJSON == null) {
            return Collections.emptyList();
        }

        JsonArray hashtags = statusJSON.getAsJsonObject("entities").getAsJsonArray("hashtags");
        if (hashtags == null) {
            return Collections.emptyList();
        }

        Date date;
        try {
            date = INPUT_DATE_FMT.parse(statusJSON.getAsJsonPrimitive("created_at").getAsString());
        } catch (ParseException e) {
            return Collections.emptyList();
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        String hour = String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));

        return StreamSupport.stream(hashtags.spliterator(), false)
                .map(hashtag -> {
                    String hashtagText = hashtag.getAsJsonObject().getAsJsonPrimitive("text").getAsString();
                    return String.join(KEY_DELIMITER, OUTPUT_DATE_FMT.format(date), hour, hashtagText);
                }).collect(Collectors.toList());
    }

    /**
     * Generates callback function that is called when record is sent or sending is failed
     *
     * @return callback
     */
    private static Callback callback() {
        return (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                LOG.error("Exception while sending message: " + exception.getMessage());
            } else {
                LOG.info("Message is sent successfully");
            }
        };
    }
}
