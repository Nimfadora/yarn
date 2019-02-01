package com.vasileva.streaming.aggregator;

import com.google.common.collect.ImmutableMap;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.vasileva.streaming.HashtagProcessor.*;

public class HashtagAggregatorMain {
    private static final Duration BATCH_INTERVAL = Minutes.apply(10);
    private static final String CHECKPOINT_DIR = "/user/root/streaming/twitter/agg";


    public static void main(String[] args) throws InterruptedException {
        String inputTopic = args[0];
        String outputTopic = args[1];
        long timeout = TimeUnit.MINUTES.toMillis(Integer.parseInt(args[2]));

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR, HashtagAggregatorMain::createContext,
                SparkHadoopUtil.get().conf(), false);

        Map<String, String> kafkaProps = ImmutableMap.of(
                "bootstrap.servers", "sandbox-hdp.hortonworks.com:6667",
                "auto.offset.reset", "smallest");
        JavaPairDStream<String, String> stats = KafkaUtils.createDirectStream(ssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaProps, Collections.singleton(inputTopic));
        sendToKafka(convertToKafkaMessageStream(aggStats(stats)), outputTopic);

        ssc.start();
        ssc.awaitTerminationOrTimeout(timeout);
    }

    private static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf().setAppName("HashtagsAggregator");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, BATCH_INTERVAL);
        ctx.checkpoint(CHECKPOINT_DIR);
        return ctx;
    }
}
