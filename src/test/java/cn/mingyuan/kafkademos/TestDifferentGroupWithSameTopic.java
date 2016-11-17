package cn.mingyuan.kafkademos;

import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

public class TestDifferentGroupWithSameTopic extends TestCase {

    @Test
    public void test() {
        PropertyConfigurator.configure("conf/log4j.properties");
        ConsumerThread consumerThread = new ConsumerThread("groupA", "g-A-1");
        ConsumerThread consumerThread2 = new ConsumerThread("groupB", "g-B-1");
        consumerThread.start();
        consumerThread2.start();

        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            consumerThread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class ConsumerThread extends Thread {
        private String clientId;
        private String groupName;

        public ConsumerThread(String groupName, String clientId) {
            this.groupName = groupName;
            this.clientId = clientId;
        }

        public void run() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "spark-master:9092,spark-slave:9092,spark-slave2:9092");
            props.put("group.id", groupName);
            props.put("enable.auto.commit", "false");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("client.id", clientId);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("my-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(groupName + " " + clientId + ",offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                }
            }
        }
    }
}
