package com.vasileva.streaming.processor;

import com.google.common.collect.ImmutableMap;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class TwitProcessorMain {
    private static final Duration BATCH_INTERVAL = Seconds.apply(10);
    private static final String CHECKPOINT_DIR = "/user/root/streaming/twitter/process";
    private static final Duration WINDOW_LENGTH = Minutes.apply(3);

    public static void main(String[] args) throws InterruptedException {
        String inputTopic = args[0];
        String outputTopic = args[1];
        long timeout = TimeUnit.MINUTES.toMillis(Integer.parseInt(args[2]));

        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(CHECKPOINT_DIR, TwitProcessorMain::createContext,
                SparkHadoopUtil.get().conf(), false);

        Map<String, String> kafkaProps = ImmutableMap.of(
                "bootstrap.servers", "sandbox-hdp.hortonworks.com:6667",
                "startingOffsets", "earliest");
        JavaPairDStream<String, String> data = KafkaUtils.createDirectStream(ssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaProps, Collections.singleton(inputTopic));
        SparkTwitProcessor.processTwits(data, outputTopic, WINDOW_LENGTH);

        ssc.start();
        ssc.awaitTerminationOrTimeout(timeout);
    }

    private static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf().setAppName("TwitProcessor");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, BATCH_INTERVAL);
        ctx.checkpoint(CHECKPOINT_DIR);
        return ctx;
    }
}
