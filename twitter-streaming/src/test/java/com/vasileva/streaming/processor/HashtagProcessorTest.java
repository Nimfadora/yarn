package com.vasileva.streaming.processor;

import com.google.common.collect.ImmutableList;
import com.vasileva.streaming.HashtagProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.vasileva.streaming.HashtagProcessor.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HashtagProcessorTest {

    private JavaSparkContext sc;
    private JavaStreamingContext ssc;

    @Before
    public void setUp() {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("test-streaming");
        ssc = new JavaStreamingContext(sparkConf, Milliseconds.apply(500));
        sc = ssc.sparkContext();
    }

    @After
    public void tearDown() {
        ssc.stop(true, false);
    }

    @Test
    public void testParseStatus() {
        String status = "{\"created_at\":\"Thu Jan 24 10:50:50 +0000 2019\",\"id\":1088388650621169666,\"id_str\":\"1088388650621169666\",\"text\":\"Promise Trump a golf course and the embargo would end tomorrow. \\nThe Embargo on Cuba Failed. Let’s Move On. https:\\/\\/t.co\\/eAJg8AsOPU\",\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\/#!\\/download\\/ipad\\\" rel=\\\"nofollow\\\">Twitter for iPad<\\/a>\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":358114912,\"id_str\":\"358114912\",\"name\":\"Bruce Edwards\",\"screen_name\":\"Vtjourno\",\"location\":\"Rutland, Vt. USA\",\"url\":null,\"description\":\"Freelance journalist\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":97,\"friends_count\":482,\"listed_count\":5,\"favourites_count\":9059,\"statuses_count\":8172,\"created_at\":\"Fri Aug 19 12:00:47 +0000 2011\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"91D2FA\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/358114912\\/1528645609\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"4e6257bfcdf04353\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/4e6257bfcdf04353.json\",\"place_type\":\"city\",\"name\":\"Rutland\",\"full_name\":\"Rutland, VT\",\"country_code\":\"US\",\"country\":\"United States\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-73.011444,43.573168],[-73.011444,43.638277],[-72.93599,43.638277],[-72.93599,43.573168]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"AI\",\"indices\":[19,22]},{\"text\":\"ArtificialIntelligence\",\"indices\":[119,142]}],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"PD_MobileApps\",\"name\":\"Peter Dyer\",\"id\":973888390050471936,\"id_str\":\"973888390050471936\",\"indices\":[3,17]},{\"screen_name\":\"indexingai\",\"name\":\"AI Index\",\"id\":928843374315966464,\"id_str\":\"928843374315966464\",\"indices\":[75,86]},{\"screen_name\":\"MikeQuindazzi\",\"name\":\"Mike Quindazzi ✨\",\"id\":2344530218,\"id_str\":\"2344530218\",\"indices\":[91,105]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1548327050248\"}";

        List<String> expected = ImmutableList.of("2019-01-24;10;AI", "2019-01-24;10;ArtificialIntelligence");
        List<String> aggregationKeys = HashtagProcessor.parseTwit(status);
        assertEquals(expected.size(), aggregationKeys.size());
        assertTrue(expected.containsAll(aggregationKeys));
    }

    @Test
    public void testParseStatusEmptyHashtags() {
        String status = "{\"created_at\":\"Thu Jan 24 10:50:50 +0000 2019\",\"id\":1088388650621169666,\"id_str\":\"1088388650621169666\",\"text\":\"Promise Trump a golf course and the embargo would end tomorrow. \\nThe Embargo on Cuba Failed. Let’s Move On. https:\\/\\/t.co\\/eAJg8AsOPU\",\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\/#!\\/download\\/ipad\\\" rel=\\\"nofollow\\\">Twitter for iPad<\\/a>\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":358114912,\"id_str\":\"358114912\",\"name\":\"Bruce Edwards\",\"screen_name\":\"Vtjourno\",\"location\":\"Rutland, Vt. USA\",\"url\":null,\"description\":\"Freelance journalist\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":97,\"friends_count\":482,\"listed_count\":5,\"favourites_count\":9059,\"statuses_count\":8172,\"created_at\":\"Fri Aug 19 12:00:47 +0000 2011\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"91D2FA\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/358114912\\/1528645609\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"4e6257bfcdf04353\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/4e6257bfcdf04353.json\",\"place_type\":\"city\",\"name\":\"Rutland\",\"full_name\":\"Rutland, VT\",\"country_code\":\"US\",\"country\":\"United States\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-73.011444,43.573168],[-73.011444,43.638277],[-72.93599,43.638277],[-72.93599,43.573168]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"PD_MobileApps\",\"name\":\"Peter Dyer\",\"id\":973888390050471936,\"id_str\":\"973888390050471936\",\"indices\":[3,17]},{\"screen_name\":\"indexingai\",\"name\":\"AI Index\",\"id\":928843374315966464,\"id_str\":\"928843374315966464\",\"indices\":[75,86]},{\"screen_name\":\"MikeQuindazzi\",\"name\":\"Mike Quindazzi ✨\",\"id\":2344530218,\"id_str\":\"2344530218\",\"indices\":[91,105]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1548327050248\"}";
        assertTrue(HashtagProcessor.parseTwit(status).isEmpty());

        String statusWithoutHashtagsField = "{\"created_at\":\"Thu Jan 24 10:50:50 +0000 2019\",\"id\":1088388650621169666,\"id_str\":\"1088388650621169666\",\"text\":\"Promise Trump a golf course and the embargo would end tomorrow. \\nThe Embargo on Cuba Failed. Let’s Move On. https:\\/\\/t.co\\/eAJg8AsOPU\",\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\/#!\\/download\\/ipad\\\" rel=\\\"nofollow\\\">Twitter for iPad<\\/a>\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":358114912,\"id_str\":\"358114912\",\"name\":\"Bruce Edwards\",\"screen_name\":\"Vtjourno\",\"location\":\"Rutland, Vt. USA\",\"url\":null,\"description\":\"Freelance journalist\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":97,\"friends_count\":482,\"listed_count\":5,\"favourites_count\":9059,\"statuses_count\":8172,\"created_at\":\"Fri Aug 19 12:00:47 +0000 2011\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"91D2FA\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/358114912\\/1528645609\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"4e6257bfcdf04353\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/4e6257bfcdf04353.json\",\"place_type\":\"city\",\"name\":\"Rutland\",\"full_name\":\"Rutland, VT\",\"country_code\":\"US\",\"country\":\"United States\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-73.011444,43.573168],[-73.011444,43.638277],[-72.93599,43.638277],[-72.93599,43.573168]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"urls\":[],\"user_mentions\":[{\"screen_name\":\"PD_MobileApps\",\"name\":\"Peter Dyer\",\"id\":973888390050471936,\"id_str\":\"973888390050471936\",\"indices\":[3,17]},{\"screen_name\":\"indexingai\",\"name\":\"AI Index\",\"id\":928843374315966464,\"id_str\":\"928843374315966464\",\"indices\":[75,86]},{\"screen_name\":\"MikeQuindazzi\",\"name\":\"Mike Quindazzi ✨\",\"id\":2344530218,\"id_str\":\"2344530218\",\"indices\":[91,105]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1548327050248\"}";
        assertTrue(HashtagProcessor.parseTwit(statusWithoutHashtagsField).isEmpty());
    }

    @Test
    public void testParseNullStatus() {
        assertTrue(HashtagProcessor.parseTwit(null).isEmpty());
    }

    @Test
    public void testAggTwitHashtags() throws InterruptedException {
        JavaRDD<String> data = sc.parallelize(ImmutableList.of(
                "userName;{\"created_at\":\"Thu Jan 24 10:50:50 +0000 2019\",\"id\":1088388650621169666,\"id_str\":\"1088388650621169666\",\"text\":\"Promise Trump a golf course and the embargo would end tomorrow. \\nThe Embargo on Cuba Failed. Let’s Move On. https:\\/\\/t.co\\/eAJg8AsOPU\",\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\/#!\\/download\\/ipad\\\" rel=\\\"nofollow\\\">Twitter for iPad<\\/a>\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":358114912,\"id_str\":\"358114912\",\"name\":\"Bruce Edwards\",\"screen_name\":\"Vtjourno\",\"location\":\"Rutland, Vt. USA\",\"url\":null,\"description\":\"Freelance journalist\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":97,\"friends_count\":482,\"listed_count\":5,\"favourites_count\":9059,\"statuses_count\":8172,\"created_at\":\"Fri Aug 19 12:00:47 +0000 2011\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"91D2FA\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/358114912\\/1528645609\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"4e6257bfcdf04353\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/4e6257bfcdf04353.json\",\"place_type\":\"city\",\"name\":\"Rutland\",\"full_name\":\"Rutland, VT\",\"country_code\":\"US\",\"country\":\"United States\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-73.011444,43.573168],[-73.011444,43.638277],[-72.93599,43.638277],[-72.93599,43.573168]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"AI\",\"indices\":[19,22]},{\"text\":\"ArtificialIntelligence\",\"indices\":[119,142]}],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"PD_MobileApps\",\"name\":\"Peter Dyer\",\"id\":973888390050471936,\"id_str\":\"973888390050471936\",\"indices\":[3,17]},{\"screen_name\":\"indexingai\",\"name\":\"AI Index\",\"id\":928843374315966464,\"id_str\":\"928843374315966464\",\"indices\":[75,86]},{\"screen_name\":\"MikeQuindazzi\",\"name\":\"Mike Quindazzi ✨\",\"id\":2344530218,\"id_str\":\"2344530218\",\"indices\":[91,105]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1548327050248\"}",
                "userName;{\"created_at\":\"Thu Jan 24 10:50:50 +0000 2019\",\"id\":1088388650621169666,\"id_str\":\"1088388650621169666\",\"text\":\"Promise Trump a golf course and the embargo would end tomorrow. \\nThe Embargo on Cuba Failed. Let’s Move On. https:\\/\\/t.co\\/eAJg8AsOPU\",\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\/#!\\/download\\/ipad\\\" rel=\\\"nofollow\\\">Twitter for iPad<\\/a>\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":358114912,\"id_str\":\"358114912\",\"name\":\"Bruce Edwards\",\"screen_name\":\"Vtjourno\",\"location\":\"Rutland, Vt. USA\",\"url\":null,\"description\":\"Freelance journalist\",\"translator_type\":\"none\",\"protected\":false,\"verified\":false,\"followers_count\":97,\"friends_count\":482,\"listed_count\":5,\"favourites_count\":9059,\"statuses_count\":8172,\"created_at\":\"Fri Aug 19 12:00:47 +0000 2011\",\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"91D2FA\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/697460351277793280\\/f1CJdZyR_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/358114912\\/1528645609\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"4e6257bfcdf04353\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/4e6257bfcdf04353.json\",\"place_type\":\"city\",\"name\":\"Rutland\",\"full_name\":\"Rutland, VT\",\"country_code\":\"US\",\"country\":\"United States\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-73.011444,43.573168],[-73.011444,43.638277],[-72.93599,43.638277],[-72.93599,43.573168]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"quote_count\":0,\"reply_count\":0,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"AI\",\"indices\":[19,22]},{\"text\":\"ArtificialIntelligence\",\"indices\":[119,142]}],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"PD_MobileApps\",\"name\":\"Peter Dyer\",\"id\":973888390050471936,\"id_str\":\"973888390050471936\",\"indices\":[3,17]},{\"screen_name\":\"indexingai\",\"name\":\"AI Index\",\"id\":928843374315966464,\"id_str\":\"928843374315966464\",\"indices\":[75,86]},{\"screen_name\":\"MikeQuindazzi\",\"name\":\"Mike Quindazzi ✨\",\"id\":2344530218,\"id_str\":\"2344530218\",\"indices\":[91,105]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1548327050248\"}"
        ));
        Queue<JavaRDD<String>> input = new LinkedList<>(ImmutableList.of(data, data));
        JavaPairDStream<String, String> dataStream = ssc.queueStream(input, false, sc.emptyRDD())
                .mapToPair(entry -> Tuple2.apply(entry.split(";")[0], entry.split(";")[1]));

        List<Tuple2<String, Long>> actual = new ArrayList<>();
        aggTwitHashtags(dataStream, Seconds.apply(1)).foreachRDD(rdd -> actual.addAll(rdd.collect()));

        ssc.start();
        ssc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(4));

        List<Tuple2<String, Long>> expected = ImmutableList.of(
                Tuple2.apply("2019-01-24;10;AI", 4L),
                Tuple2.apply("2019-01-24;10;ArtificialIntelligence", 4L)
        );
        assertEquals(expected.size(), actual.size());
        assertTrue(expected.containsAll(actual));
    }

    @Test
    public void testAggStats() throws InterruptedException {
        JavaRDD<String> data = sc.parallelize(ImmutableList.of(
                "2019-01-24;10,2019-01-24;10;AI;2",
                "2019-01-24;10,2019-01-24;10;ArtificialIntelligence;3",
                "2019-01-24;10,2019-01-24;10;AI;1",
                "2019-01-24;10,2019-01-24;10;ArtificialIntelligence;1"));
        Queue<JavaRDD<String>> input = new LinkedList<>(ImmutableList.of(data, data));
        JavaPairDStream<String, String> dataStream = ssc.queueStream(input, false, sc.emptyRDD())
                .mapToPair(entry -> Tuple2.apply(entry.split(",")[0], entry.split(",")[1]));

        List<Tuple2<String, Long>> actual = new ArrayList<>();
        aggStats(dataStream).foreachRDD(rdd -> actual.addAll(rdd.collect()));

        ssc.start();
        ssc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(2));

        List<Tuple2<String, Long>> expected = ImmutableList.of(
                Tuple2.apply("2019-01-24;10;AI", 6L),
                Tuple2.apply("2019-01-24;10;ArtificialIntelligence", 8L)
        );
        assertEquals(expected.size(), actual.size());
        assertTrue(expected.containsAll(actual));
    }

    @Test
    public void testConvertToKafkaMessageStream() throws InterruptedException {
        JavaRDD<Tuple2<String, Long>> data = sc.parallelize(ImmutableList.of(
                Tuple2.apply("2019-01-24;10;AI", 2L),
                Tuple2.apply("2019-01-25;10;ArtificialIntelligence", 3L),
                Tuple2.apply("2019-03-24;10;AI", 1L),
                Tuple2.apply("2019-01-25;12;ArtificialIntelligence", 4L)
        ));
        Queue<JavaRDD<Tuple2<String, Long>>> input = new LinkedList<>(ImmutableList.of(data));
        JavaPairDStream<String, Long> dataStream = ssc.queueStream(input, false, sc.emptyRDD())
                .mapToPair(entry -> entry);

        List<Tuple2<String, String>> actual = new ArrayList<>();
        convertToKafkaMessageStream(dataStream).foreachRDD(rdd -> actual.addAll(rdd.collect()));

        ssc.start();
        ssc.awaitTerminationOrTimeout(TimeUnit.SECONDS.toMillis(2));

        List<Tuple2<String, String>> expected = ImmutableList.of(
                Tuple2.apply("2019-01-24;10", "2019-01-24;10;AI;2"),
                Tuple2.apply("2019-01-25;10", "2019-01-25;10;ArtificialIntelligence;3"),
                Tuple2.apply("2019-03-24;10", "2019-03-24;10;AI;1"),
                Tuple2.apply("2019-01-25;12", "2019-01-25;12;ArtificialIntelligence;4")
        );
        assertEquals(expected.size(), actual.size());
        assertTrue(expected.containsAll(actual));
    }
}
