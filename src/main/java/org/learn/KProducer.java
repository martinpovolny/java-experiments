package org.learn;

import java.util.Properties;
//import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KProducer {

    private final String bootstrapServers = "localhost:9092";
    private final String topic = "my-topic";

    public void produce(String messageContent) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        Producer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        Message.MyMessage msg = Message.MyMessage.newBuilder().
                setContent(messageContent).
                build();

        producer.send(new ProducerRecord<>(topic, msg.toByteArray()));
        producer.close();
        System.out.println("Message sent successfully!");
    }
}