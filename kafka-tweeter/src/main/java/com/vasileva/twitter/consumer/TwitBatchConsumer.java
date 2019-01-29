package com.vasileva.twitter.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Consumes data from Kafka twit topic in a batch mode, aggregates counts by hashtags in hourly manner,
 * saves results to HDFS partitioned by date and hour.
 */
class TwitBatchConsumer {
    private static final String BOOTSTRAP_SERVERS = "sandbox-hdp.hortonworks.com:6667";
    private static final Logger LOG = LoggerFactory.getLogger(TwitBatchConsumer.class);

    public static StructType HASHTAG_STATS_SCHEMA = new StructType(new StructField[]{
            new StructField("date", StringType, true, Metadata.empty()),
            new StructField("hour", StringType, true, Metadata.empty()),
            new StructField("hashtag", StringType, true, Metadata.empty()),
            new StructField("cnt", LongType, true, Metadata.empty())
    });

    private final SparkSession spark;
    private final FileSystem fs;
    private final String topic;
    private final String outputDir;
    private final String tmpDir;
    private final String initialOffset;
    private final long partitionBatchSize;
    private final long timeout;

    TwitBatchConsumer(SparkSession spark, FileSystem fs, String baseDir, String topic,
                      String initialOffset, long partitionBatchSize, long timeout) throws IOException {
        this.spark = spark;
        this.fs = fs;
        this.outputDir = new Path(baseDir, "hashtags").toString();
        this.tmpDir = new Path(baseDir, "tmp").toString();
        this.topic = topic;
        this.initialOffset = initialOffset;
        this.partitionBatchSize = partitionBatchSize;
        this.timeout = timeout;
        spark.udf().register("getHashtagText", GET_HASHTAG_TEXT);
        fs.mkdirs(new Path(outputDir));
    }

    void start() throws InterruptedException, IOException {
        long endTime = System.currentTimeMillis() + timeout;
        String startOffset = initialOffset;
        String endOffset = OffsetUtils.getEndOffsets(startOffset, partitionBatchSize);

        while (System.currentTimeMillis() < endTime) {
            LOG.info("Processing batch");
            Dataset<Row> rawData = readBatch(startOffset, endOffset);

            if (rawData.count() == 0) {
                Thread.sleep(2000);
                continue;
            }

            Dataset<Row> batchStats = aggStats(rawData).cache();
            List<String> partitionPaths = getPartitionPaths(batchStats);
            Dataset<Row> archivedStats = getArchivedStats(partitionPaths);

            writeStats(mergeArchiveStats(batchStats, archivedStats), partitionPaths);

            startOffset = endOffset;
            endOffset = OffsetUtils.getEndOffsets(startOffset, partitionBatchSize);

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

    /**
     * Fetching paths of partitions that are updated in the current batch.
     *
     * @param batchStats current batch
     * @return partition paths
     */
    @VisibleForTesting
    public List<String> getPartitionPaths(Dataset<Row> batchStats) {
        return batchStats.select("date", "hour").distinct()
                .collectAsList()
                .stream()
                .map(r -> String.join("/", outputDir, "date=" + r.getString(0), "hour=" + r.getString(1)))
                .filter(this::fileExists)
                .collect(Collectors.toList());
    }

    /**
     * Reading partitions that should be updated during the current batch. Reading the whole data is not optimal,
     * as post-processing after join by partitions will be harder than after union.
     *
     * @param paths path to partitions
     * @return partitions data
     */
    @VisibleForTesting
    public Dataset<Row> getArchivedStats(List<String> paths) {
        Seq<String> filePaths = JavaConverters.asScalaIteratorConverter(paths.iterator()).asScala().toSeq();
        return spark.read()
                .schema(HASHTAG_STATS_SCHEMA)
                .option("basePath", outputDir) // to preserve partition columns
                .parquet(filePaths)
                .select("date", "hour", "hashtag", "cnt");
    }

    /**
     * Parsing JSON twitter status, getting hashtags from it and aggregating stats by date, hour and hashtag text.
     *
     * @param batchData Dataset with Kafka messages
     * @return Dataset with counts by hashtag, date and hour
     * @see TwitBatchConsumer#HASHTAG_STATS_SCHEMA
     */
    @VisibleForTesting
    public Dataset<Row> aggStats(Dataset<Row> batchData) {
        return batchData.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG)")
                .withColumn("hashtags_array", get_json_object(col("value"), "$.entities.hashtags"))
                .withColumn("hashtag", explode(callUDF("getHashtagText", col("hashtags_array"))))
                .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd"))
                .withColumn("hour", from_unixtime(col("timestamp"), "HH"))
                .select(col("hashtag"), col("date"), col("hour"), lit(1).as("cnt"))
                .groupBy(col("hashtag"), col("date"), col("hour"))
                .agg(sum("cnt").as("cnt"))
                .select("date", "hour", "hashtag", "cnt");

    }

    @VisibleForTesting
    public Dataset<Row> mergeArchiveStats(Dataset<Row> batchData, Dataset<Row> archivedData) {
        return archivedData.union(batchData)
                .groupBy("date", "hour", "hashtag")
                .agg(sum("cnt").as("cnt"));
    }

    /**
     * As we re-reading previous partitions in the same job we cannot overwrite data at once, as it is the source
     * of DAG. We have to save data to temporary directory, re-read it and then overwrite the archived partitions.
     *
     * @param stats stats to be written
     * @param previousStatsPaths path to archive partitions which must be overwritten
     * @throws IOException if exception occurres while writing
     */
    @VisibleForTesting
    public void writeStats(Dataset<Row> stats, List<String> previousStatsPaths) throws IOException {
        // write to temp directory
        fs.delete(new Path(tmpDir), true);
        stats.write().partitionBy("date", "hour").parquet(tmpDir);

        // delete previous stats partitions, as overwrite mode just overwrites the whole base directory,
        // don't preserving the partitions that were not updated
        for (String partitionPath : previousStatsPaths) {
            fs.delete(new Path(partitionPath), true);
        }

        // write the result to the result directory, overwriting archived partitions
        spark.read().schema(HASHTAG_STATS_SCHEMA)
                .parquet(tmpDir)
                .write()
                .partitionBy("date", "hour")
                .mode(SaveMode.Append).parquet(outputDir);

        fs.delete(new Path(tmpDir), true);
    }

    private boolean fileExists(String path) {
        try {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            throw new IllegalStateException("Exception occurred while fetching partitions: " + e.getMessage(), e);
        }
    }

    /**
     * Parses JSON array hashtags string:
     * "[{"text":"AI","indices":[19,22]},{"text":"ArtificialIntelligence","indices":[119,142]}]"
     * to array of hashtags:
     * ["AI", "ArtificialIntelligence"]
     */
    private static UserDefinedFunction GET_HASHTAG_TEXT = udf((UDF1<String, String[]>) hashtagString -> {
        Gson mapper = new Gson();
        if (hashtagString == null || hashtagString.isEmpty() || hashtagString.equals("[]")) {
            return null;
        }
        try {
            List<Hashtag> hashtagObjects = mapper.fromJson(hashtagString, new TypeToken<List<Hashtag>>() {}.getType());
            return hashtagObjects.stream().map(e -> e.text).toArray(String[]::new);
        } catch (JsonSyntaxException e) {
            return null;
        }
    }, DataTypes.createArrayType(DataTypes.StringType));
}
