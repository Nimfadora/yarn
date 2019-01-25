package com.vasileva.twitter.producer;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import twitter4j.FilterQuery;
import twitter4j.StreamListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

/**
 * Streams twits, filters them by keywords and location and sends them to Kafka topic specified
 */
public class TwitProducerMain {
    private static final String[] KEYWORDS = new String[]{"big", "data", "ai", "machine", "learning", "course"};
    private static final String CONSUMER_KEY = "sS38txzAyOiIKLWRdw0PcJMXK";
    private static final String CONSUMER_SECRET = "vIVZr96yWc4QjJoO1rnQ84ijKQfpsjidEJy2Jiw5ZRFDyhZyiO";
    private static final String ACCESS_TOKEN = "943543787078520833-m9nrevimcWoTukB8wPnAl8EccuJoRP5";
    private static final String ACCESS_TOKEN_SECRET = "LR7QlHpyfIMrwdZ77ae4xXA4MROaDqOiWciXCGFz26LzW";


    public static void main(String[] args) {
        Preconditions.checkArgument(args.length == 1, "Application accepts only single: topic");

        String topic = args[0];

        TwitterStream twitterStream = new TwitterStreamFactory(getTwitterStreamConfig()).getInstance();

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProducerProperties());
        StreamListener listener = new TwitProducer(topic, kafkaProducer, twitterStream);
        twitterStream.addListener(listener);
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(KEYWORDS);
        twitterStream.filter(filterQuery);
    }

    private static Configuration getTwitterStreamConfig() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(CONSUMER_KEY);
        cb.setOAuthConsumerSecret(CONSUMER_SECRET);
        cb.setOAuthAccessToken(ACCESS_TOKEN);
        cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);
        return cb.build();
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
