package com.vasileva.streaming.aggregator;

import com.google.common.collect.ImmutableMap;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.*;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class HashtagAggregatorMain {
    private static Logger LOG = LoggerFactory.getLogger(HashtagAggregatorMain.class);
    private static final Duration BATCH_INTERVAL = Minutes.apply(10);
    private static final String CHECKPOINT_DIR = "/user/root/streaming/twitter/agg";
    private static final Properties KAFKA_PRODUCER_PROPS = getProducerProperties();

    public static void main(String[] args) throws InterruptedException {
        String inputTopic = args[0];
        String outputTopic = args[1];
        long timeout = TimeUnit.MINUTES.toMillis(Integer.parseInt(args[2]));

        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR, HashtagAggregatorMain::createContext,
                SparkHadoopUtil.get().conf(), false);

        Map<String, String> kafkaProps = ImmutableMap.of(
                "bootstrap.servers", "sandbox-hdp.hortonworks.com:6667",
                "startingOffsets", "earliest");
        JavaPairDStream<String, String> data = KafkaUtils.createDirectStream(ssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaProps, Collections.singleton(inputTopic));
        aggregateHashtags(data, outputTopic);

        ssc.start();
        ssc.awaitTerminationOrTimeout(timeout);
        producer.close();
    }

    public static void aggregateHashtags(JavaPairDStream<String, String> data, String topic) {
        data.reduceByKey((v1, v2) -> v1 + v2).foreachRDD(rdd -> rdd.foreachPartition(p -> {
            KafkaProducer<String, String> producer = new KafkaProducer<>(KAFKA_PRODUCER_PROPS);
            p.forEachRemaining(hashtag2count ->
                    producer.send(new ProducerRecord<>(topic, hashtag2count._1, String.valueOf(hashtag2count._2)), callback()));
            producer.close();
        }));
    }

    private static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf().setAppName("HashtagsAggregator");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, BATCH_INTERVAL);
        ctx.checkpoint(CHECKPOINT_DIR);
        return ctx;
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
