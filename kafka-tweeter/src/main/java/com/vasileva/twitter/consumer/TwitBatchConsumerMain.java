package com.vasileva.twitter.consumer;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Reads data from Kafka topic in batch mode, saves it to HDFS
 */
public class TwitBatchConsumerMain {

    public static void main(String[] args) throws InterruptedException, IOException {
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

        FileSystem fs = FileSystem.get(new Configuration());

        try (SparkSession spark = SparkSession.builder().appName("BatchConsumer").getOrCreate()) {
            TwitBatchConsumer consumer = new TwitBatchConsumer(spark, fs, outputDir, topic, offsetString, batchSize / partitionsCount, timeout);
            consumer.start();
        }
    }


}