package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.PropertyConfigurator;

import java.util.Arrays;
import java.util.Properties;

/**
 * 多线程读取同一个topic的数据，互不干扰
 */
public class TestDifferentGroupWithSameTopic {

    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        ConsumerThread consumerThread = new ConsumerThread("group12317", "g-A-1");
        ConsumerThread consumerThread2 = new ConsumerThread("group22271", "g-B-1");
        consumerThread.start();
        consumerThread2.start();
        System.out.println("-----------------1");
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
            props.put("bootstrap.servers", "172.16.151.179:9092,172.16.151.179:9093,172.16.151.179:9094");
            props.put("group.id", groupName);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put("client.id", clientId);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            final String topic = "my-topic112300";
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("groupName=%s, clientId=%s ,partition=%s,offset = %d, key = %s, value = %s %n", groupName, clientId, record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
}
