package com.vasileva.twitter.consumer;

import com.google.common.annotations.VisibleForTesting;
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
import org.codehaus.jackson.map.ObjectMapper;
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
    private final String initialOffset;
    private final long partitionBatchSize;
    private final long timeout;

    TwitBatchConsumer(SparkSession spark, FileSystem fs, String outputDir, String topic,
                      String initialOffset, long partitionBatchSize, long timeout) {
        this.spark = spark;
        this.fs = fs;
        this.outputDir = outputDir;
        this.topic = topic;
        this.initialOffset = initialOffset;
        this.partitionBatchSize = partitionBatchSize;
        this.timeout = timeout;
        spark.udf().register("getHashtagText", GET_HASHTAG_TEXT);
    }

    void start() throws InterruptedException, IOException {
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

            Dataset<Row> batchStats = aggStats(rawData).cache();
            List<String> partitionPaths = getPartitionPaths(batchStats);
            Dataset<Row> archivedStats = getArchivedStats(partitionPaths);

            writeData(mergeArchiveStats(batchStats, archivedStats), partitionPaths);

            startOffset = endOffset;
            endOffset = OffsetConverter.getEndOffset(startOffset, partitionBatchSize);

            LOG.info("Start offsets: " + startOffset);
            LOG.info("End offsets: " + endOffset);
            LOG.info("Batch processing finished");
            Thread.sleep(2000);
        }
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

    private boolean fileExists(String path) {
        try {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            throw new IllegalStateException("Exception occurred while fetching partitions: " + e.getMessage(), e);
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
     * Reading partitions that should be updated during the current batch.
     *
     * @param paths path to partitions
     * @return partitions data
     */
    @VisibleForTesting
    public Dataset<Row> getArchivedStats(List<String> paths) {
        Seq<String> filePaths = JavaConverters.asScalaIteratorConverter(paths.iterator()).asScala().toSeq();
        return spark.read()
                .schema(HASHTAG_STATS_SCHEMA)
                .option("basePath", outputDir)
                .parquet(filePaths)
                .select("date", "hour", "hashtag", "cnt");
    }

    @VisibleForTesting
    public Dataset<Row> aggStats(Dataset<Row> rawData) {
        return rawData.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS LONG)")
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
     * @param data
     * @throws IOException
     */
    @VisibleForTesting
    public void writeData(Dataset<Row> data, List<String> partitionsPath) throws IOException {
        Path tmpPath = new Path(fs.getHomeDirectory(), "tmp/");

        // write to temp directory
        data.write().partitionBy("date", "hour").parquet(tmpPath.toString());

        partitionsPath.forEach(p -> {
            try {
                fs.delete(new Path(p), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        // write the result to the result directory, overwriting archived partitions
        spark.read().schema(HASHTAG_STATS_SCHEMA)
                .parquet(tmpPath.toString())
                .write()
                .partitionBy("date", "hour")
                .mode(SaveMode.Append).parquet(outputDir);
        fs.delete(tmpPath, true);
//        FileUtil.copy(fs, new Path(tmpPath + "/*"), fs, new Path(outputDir), true, true, fs.getConf());
    }

    /**
     * Parses JSON array hashtags string:
     * "[{"text":"AI","indices":[19,22]},{"text":"ArtificialIntelligence","indices":[119,142]}]"
     * to array of hashtags:
     * ["AI", "ArtificialIntelligence"]
     */
    private static UserDefinedFunction GET_HASHTAG_TEXT = udf((UDF1<String, String[]>) hashtagString -> {
        ObjectMapper mapper = new ObjectMapper();
        if (hashtagString == null || hashtagString.isEmpty() || hashtagString.equals("[]")) {
            return null;
        }
        try {
            List<Hashtag> hashtagObjects = mapper.readValue(hashtagString, mapper.getTypeFactory().constructCollectionType(List.class, Hashtag.class));
            return hashtagObjects.stream().map(e -> e.text).toArray(String[]::new);
        } catch (IOException e) {
            return null;
        }
    }, DataTypes.createArrayType(DataTypes.StringType));
}
