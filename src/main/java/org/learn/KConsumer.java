package org.learn;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KConsumer {
    private final String bootstrapServers = "localhost:9092";
    private final String groupId = "my-consumer-group";
    private final String topic = "my-topic";

    public void consume() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                try {
                    Message.MyMessage myMessage = Message.MyMessage.parseFrom(record.value());
                    System.out.println("Received: " + myMessage.getContent());
                } catch (Exception e) {
                    System.out.println("Error decoding message: " + e.getMessage());
                }
            });
        }
    }

    public static void main(String[] args) {
        new KConsumer().consume();
    }
}

