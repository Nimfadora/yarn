package com.vasileva.streaming.processor;

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

public class SparkTwitProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SparkTwitProcessor.class);
    private static final Properties KAFKA_PRODUCER_PROPS = getProducerProperties();

    /* parsing format: "Thu Jan 24 10:50:50 +0000 2019" */
    private static SimpleDateFormat INPUT_DATE_FMT = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy", Locale.US);
    private static SimpleDateFormat OUTPUT_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd", Locale.US);

    private static final String KEY_DELIMITER = ";";

    public static void processTwits(JavaPairDStream<String, String> data, String topic, Duration windowLength) {
        JavaPairDStream<String, Long> hashtag2counts = data.flatMapToPair((k2v) -> parseStatus(k2v._2)
                .stream()
                .map(key -> Tuple2.apply(key, 1L))
                .iterator());
        hashtag2counts.reduceByKeyAndWindow((cnt1, cnt2) -> cnt1 + cnt2, windowLength, windowLength)
                .foreachRDD(rdd -> rdd.foreachPartition(p -> {
                    KafkaProducer<String, String> producer = new KafkaProducer<>(KAFKA_PRODUCER_PROPS);
                    p.forEachRemaining(hashtag2count -> producer.send(new ProducerRecord<>(topic,
                            hashtag2count._1, String.valueOf(hashtag2count._2)), callback()));
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
    public static List<String> parseStatus(String twitterStatus) {
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

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
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
