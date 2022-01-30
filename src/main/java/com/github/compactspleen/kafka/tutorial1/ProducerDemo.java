package com.github.compactspleen.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static final String BOOTSTRAP_SERVER_HOST = "localhost:9092";

    public static void main(String[] args) {
        // Create Producer properties
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // Create a producer record to send
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello, world from " + ProducerDemo.class.getName());

        // send data - asynchronous
        kafkaProducer.send(producerRecord);

        // flush data
        kafkaProducer.flush();

        // flush data and close
        kafkaProducer.close();
    }
}
