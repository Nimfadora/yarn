package com.vasileva.twitter.consumer;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

/**
 * Reads data from Kafka topic in batch mode, saves it to HDFS
 */
public class TwitBatchConsumerMain {

    public static void main(String[] args) throws InterruptedException {
        Preconditions.checkArgument(args.length == 4, "Illegal number of args, should be 3: offset, outputDir, batchSize");

        String offsetString = args[0];
        String outputDir = args[1];
        long batchSize = Long.parseLong(args[2]);
        long timeout = TimeUnit.MINUTES.toMillis(Integer.parseInt(args[3]));

        OffsetConverter.Offset offset = OffsetConverter.parseFromJSONString(offsetString);
        String topic = offset.topic2partitions.keySet().iterator().next();
        int partitionsCount = offset.topic2partitions.get(topic).size();

        Preconditions.checkArgument(batchSize % partitionsCount == 0,
                "Illegal batch size, it should be common multiple of partitions count");
        try (SparkSession spark = SparkSession.builder().appName("BatchConsumer").getOrCreate()) {
            TwitBatchConsumer consumer = new TwitBatchConsumer(spark, outputDir, topic, offsetString, batchSize / partitionsCount, timeout);
            consumer.start();
        }
    }


}