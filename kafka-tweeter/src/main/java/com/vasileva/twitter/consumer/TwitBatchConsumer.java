package com.vasileva.twitter.consumer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

class TwitBatchConsumer {
    private static final String BOOTSTRAP_SERVERS = "sandbox-hdp.hortonworks.com:6667";
    private static final Logger LOG = LoggerFactory.getLogger(TwitBatchConsumer.class);
    private static final Seq<String> JOIN_KEYS = JavaConverters
            .asScalaIteratorConverter(Arrays.asList("date", "hour", "hashtag").iterator())
            .asScala()
            .toSeq();

    private final SparkSession spark;
    private final String topic;
    private final String outputDir;
    private final String initialOffset;
    private final long partitionBatchSize;
    private final long timeout;

    TwitBatchConsumer(SparkSession spark, String outputDir, String topic,
                      String initialOffset, long partitionBatchSize, long timeout) {
        this.spark = spark;
        this.outputDir = outputDir;
        this.topic = topic;
        this.initialOffset = initialOffset;
        this.partitionBatchSize = partitionBatchSize;
        this.timeout = timeout;
    }

    void start() throws InterruptedException {
        long endTime = System.currentTimeMillis() + timeout;
        String startOffset = initialOffset;
        String endOffset = OffsetConverter.getEndOffset(startOffset, partitionBatchSize);

        while (System.currentTimeMillis() < endTime) {
            LOG.info("Processing batch");
            Dataset<Row> rawData = readBatch(startOffset, endOffset);

            if (rawData.count() == 0) {
                Thread.sleep(2000);
                continue;
            }

            Dataset<Row> batchStats = aggStats(rawData);
            Dataset<Row> archivedStats = getArchivedStats();

            writeData(mergeStats(batchStats, archivedStats));

            startOffset = endOffset;
            endOffset = OffsetConverter.getEndOffset(startOffset, partitionBatchSize);

            LOG.info("Start offsets: " + startOffset);
            LOG.info("End offsets: " + endOffset);
            LOG.info("Batch processing finished");
            Thread.sleep(2000);
        }
    }

    private Dataset<Row> readBatch(String startOffset, String endOffset) {
        return spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .option("startingOffsets", startOffset)
                .option("endingOffsets", endOffset)
                .load();
    }

    @VisibleForTesting
    public Dataset<Row> aggStats(Dataset<Row> rawData) {
        return rawData.select(col("value").cast(DataTypes.StringType).as("value"))
                .withColumn("hashtag", get_json_object(col("value"), "$.entities.hashtags"));

    }

    @VisibleForTesting
    public Dataset<Row> mergeStats(Dataset<Row> batchData, Dataset<Row> archivedData) {
        return batchData.alias("batchData").join(archivedData.alias("archivedData"), JOIN_KEYS, "left")
                .withColumn("finalCount",
                        when(col("archivedData.count").isNull(), col("batchData.count")
                                .otherwise(col("batchData.count").$plus("archivedData.count"))))
                .select(col("batchData.date"), col("batchData.hour"), col("batchData.hashtag"), col("finalCount").as("count"));
    }

    private Dataset<Row> getArchivedStats() {
        return spark.read().format("parquet").load(outputDir);
    }

    @VisibleForTesting
    private void writeData(Dataset<Row> data) {
        data.write().format("parquet").partitionBy("date", "hour").mode(SaveMode.Overwrite).save(outputDir);
    }
}
