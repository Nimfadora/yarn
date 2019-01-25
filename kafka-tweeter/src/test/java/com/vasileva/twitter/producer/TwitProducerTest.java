package com.vasileva.twitter.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TwitProducerTest {
    private MockProducer<String, String> kafkaProducer;

    @Before
    public void setUp() {
        kafkaProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void testSend() throws IOException, TwitterException {
        byte[] encoded = Files.readAllBytes(new File(TwitProducer.class.getResource("/twit.json").getPath()).toPath());
        String msg = new String(encoded, UTF_8);
        Status status = (Status) TwitterObjectFactory.createObject(msg);
        TwitProducer producer = new TwitProducer("test", kafkaProducer, null);

        producer.onStatus(status);
        List<ProducerRecord<String, String>> producedRecords = kafkaProducer.history();

        assertEquals(1, producedRecords.size());
        ProducerRecord<String, String> record = producedRecords.get(0);

        assertEquals("test", record.topic());
        assertEquals("Bruce Edwards", record.key());
        assertEquals( msg, record.value());
    }

    @Test
    public void testUnknownLocation() throws IOException, TwitterException {
        byte[] encoded = Files.readAllBytes(new File(TwitProducer.class.getResource("/twit_unknown_location.json").getPath()).toPath());
        Status status = (Status) TwitterObjectFactory.createObject(new String(encoded, UTF_8));
        TwitProducer producer = new TwitProducer("test", kafkaProducer, null);

        producer.onStatus(status);
        assertTrue(kafkaProducer.history().isEmpty());
    }
}
