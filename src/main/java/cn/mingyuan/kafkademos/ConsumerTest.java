package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.PropertyConfigurator;

import java.util.*;

public class ConsumerTest {

    public static void autoOffsetCommitting() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "spark-master:9092,spark-slave:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    public static void manualOffsetControl() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "spark-master:9092,spark-slave:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        final int minBatchSize = 200;
//        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        // while (true) {
        // ConsumerRecords<String, String> records = consumer.poll(100);
        // for (ConsumerRecord<String, String> record : records) {
        // buffer.add(record);
        // }
        // if (buffer.size() >= minBatchSize) {
        // //insertIntoDb(buffer);
        // consumer.commitSync();
        // buffer.clear();
        // }
        // }
        //
        // String topic = "foo";
        // TopicPartition partition0 = new TopicPartition(topic, 0);
        // TopicPartition partition1 = new TopicPartition(topic, 1);
        // consumer.assign(Arrays.asList(partition0, partition1));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void getALL() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "spark-master:9092,spark-slave:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // consumer.subscribe(Arrays.asList("my-topic"));
        List<PartitionInfo> partitionsFor = consumer.partitionsFor("my-topic");
        if (partitionsFor != null && partitionsFor.size() > 0) {
            partitionsFor.forEach(e -> {
                int partitionId = e.partition();
                System.out.println(partitionId);
            });
        }
        consumer.subscribe(Arrays.asList("my-topic"));
        TopicPartition topicPartition = new TopicPartition("my-topic", 0);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0l);
        consumer.commitSync(Collections.singletonMap(topicPartition, offsetAndMetadata));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
        consumerRecords.forEach(e -> {
            long offset = e.offset();
            String key = e.key();
            String value = e.value();
            int partition = e.partition();
            System.out.println(partition + "," + offset + "," + key + "," + value);
        });
        consumer.close();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        // autoOffsetCommitting();
//        getALL();
        System.out.printf("a-%ne-b");
    }
}
