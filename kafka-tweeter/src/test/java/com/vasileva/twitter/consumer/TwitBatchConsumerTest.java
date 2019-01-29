package com.vasileva.twitter.consumer;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.vasileva.twitter.consumer.TwitBatchConsumer.HASHTAG_STATS_SCHEMA;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TwitBatchConsumerTest {

    @ClassRule
    public static final TemporaryFolder baseDir = new TemporaryFolder();

    private static MiniDFSCluster dfsCluster;

    private StructType kafkaRecordSchema = new StructType(new StructField[]{
            new StructField("key", StringType, true, Metadata.empty()),
            new StructField("value", StringType, true, Metadata.empty()),
            new StructField("topic", StringType, true, Metadata.empty()),
            new StructField("partition", IntegerType, true, Metadata.empty()),
            new StructField("offset", LongType, true, Metadata.empty()),
            new StructField("timestamp", LongType, true, Metadata.empty()),
            new StructField("timestampType", IntegerType, true, Metadata.empty())
    });

    private static SparkSession spark;
    private static TwitBatchConsumer consumer;
    private static FileSystem fs;
    private String outputPath;


    @BeforeClass
    public static void init() throws IOException {
        baseDir.create();
        Configuration configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getRoot().getAbsolutePath());
        configuration.set("io.compression.codecs", "org.apache.parquet.hadoop.codec.SnappyCodec");
        dfsCluster = new MiniDFSCluster.Builder(configuration).build();
        dfsCluster.waitActive();
        fs = dfsCluster.getFileSystem();
        spark = SparkSession.builder().appName("Batch consumer test").master("local[*]").getOrCreate();
    }

    @Before
    public void setUp() {
        outputPath = new Path(fs.getHomeDirectory(), "tweeter-hashtags").toString();
        consumer = new TwitBatchConsumer(spark, fs, outputPath, "test-topic", "{'topic1':{'0':0}}", 3, 5);
    }

    @AfterClass
    public static void tearDown() {
        spark.close();
    }

    @Test
    public void testAggStats() {
        String filename = TwitBatchConsumerTest.class.getResource("/batch.txt").getPath();
        Dataset<Row> data = spark.read().schema(kafkaRecordSchema).option("delimiter", ";").option("quote", "").csv(filename);
        List<String> expected = ImmutableList.of("2018-12-21,12,ArtificialIntelligence,6",
                "2018-12-21,13,AI,3",
                "2018-12-21,13,ArtificialIntelligence,3",
                "2018-12-21,12,AI,6");
        List<String> actual = collectStatsToString(consumer.aggStats(data));

        assertEquals(expected.size(), actual.size());
        assertTrue(expected.containsAll(actual));
    }

    @Test
    public void testMergeArchiveStats() throws IOException {
        String previousBatchStats = TwitBatchConsumerTest.class.getResource("/stats_1.csv").getPath();
        Dataset<Row> previousStats = spark.read().schema(HASHTAG_STATS_SCHEMA).csv(previousBatchStats);
        consumer.writeStats(previousStats, Collections.emptyList());

        assertTrue(fs.exists(new Path(outputPath)));
        assertTrue(fs.exists(new Path(outputPath, "date=2018-12-20/hour=11")));
        assertTrue(fs.exists(new Path(outputPath, "date=2018-12-21/hour=12")));
        assertTrue(fs.exists(new Path(outputPath, "date=2018-12-21/hour=13")));
        assertTrue(fs.exists(new Path(outputPath, "date=2018-12-21/hour=14")));
        assertTrue(fs.exists(new Path(outputPath, "date=2018-12-22/hour=13")));

        String statsFileName = TwitBatchConsumerTest.class.getResource("/stats_2.csv").getPath();
        Dataset<Row> stats = spark.read().schema(HASHTAG_STATS_SCHEMA).csv(statsFileName);

        List<String> partitionsPaths = consumer.getPartitionPaths(stats);
        Dataset<Row> archivedStats = consumer.getArchivedStats(partitionsPaths);
        Dataset<Row> resultStats = consumer.mergeArchiveStats(stats, archivedStats);

        List<String> expectedStats = ImmutableList.of(
                "2018-12-21,12,AI,8",
                "2018-12-21,12,ArtificialIntelligence,6",
                "2018-12-22,13,AI,3",
                "2018-12-22,13,ArtificialIntelligence,8",
                "2018-12-22,13,BigData,2",
                "2018-12-22,13,MachineLearning,2"
        );
        List<String> actualStats = collectStatsToString(resultStats);

        assertEquals(6, actualStats.size());
        assertTrue(expectedStats.containsAll(actualStats));

        consumer.writeStats(resultStats, partitionsPaths);

        Dataset<Row> wholeDataset = consumer.getArchivedStats(Collections.singletonList(new Path(outputPath).toString()));
        List<String> wholeData = collectStatsToString(wholeDataset);

        List<String> expectedData = ImmutableList.of(
                "2018-12-21,12,ArtificialIntelligence,6",
                "2018-12-22,13,ArtificialIntelligence,8",
                "2018-12-21,13,AI,1",
                "2018-12-21,13,ArtificialIntelligence,7",
                "2018-12-22,13,MachineLearning,2",
                "2018-12-21,14,BigData,3",
                "2018-12-22,13,BigData,2",
                "2018-12-21,12,AI,8",
                "2018-12-22,13,AI,3",
                "2018-12-20,11,AI,5"
        );

        assertEquals(expectedData.size(), wholeData.size());
        assertTrue(expectedData.containsAll(wholeData));
    }


    private List<String> collectStatsToString(Dataset<Row> data) {
        List<Row> res =  data.collectAsList();
        return res.stream()
                .map(r -> String.join(",", r.getString(0),  r.getString(1), r.getString(2), String.valueOf(r.getLong(3))))
                .collect(Collectors.toList());
    }


}