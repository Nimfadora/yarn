package com.vasileva.twitter.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

/**
 * Listens to the twit stream, filters only those from USA and sends them to Kafka, partitioned by twitter user name.
 */
public class TwitProducer implements StatusListener {
    private static final Logger LOG = LoggerFactory.getLogger(TwitProducer.class);

    private Producer<String, String> kafkaProducer;

    private TwitterStream twitterStream;
    private String kafkaTopic;

    public TwitProducer(String kafkaTopic, Producer<String, String> kafkaProducer, TwitterStream twitterStream) {
        this.kafkaTopic = kafkaTopic;
        this.kafkaProducer = kafkaProducer;
        this.twitterStream = twitterStream;
    }

    /**
     * Listens to a Twitter stream of records filters them be location and sends to a Kafka topic.
     *
     * @param status twitter status
     */
    @Override
    public void onStatus(Status status) {
        Place place = status.getPlace();
        if (place == null || !place.getCountryCode().equals("US")) {
            return;
        }

        String record = TwitterObjectFactory.getRawJSON(status);
        LOG.info("Received record: course " + record);

        ProducerRecord<String, String> msg = new ProducerRecord<>(kafkaTopic, status.getUser().getName(), record);
        kafkaProducer.send(msg, callback());
    }

    @Override
    public void onException(Exception ex) {
        LOG.error("Exception occurred while processing Twitter stream: " + ex.getMessage());
        LOG.info("Shutting down Twitter listener stream...");
        twitterStream.shutdown();
    }

    /**
     * Generates callback function that is called when record is sent or sending is failed
     *
     * @return callback
     */
    private Callback callback() {
        return (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                LOG.error("Exception while sending message: " + exception.getMessage());
            } else {
                LOG.info("Message is sent successfully");
            }
        };
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }
}
