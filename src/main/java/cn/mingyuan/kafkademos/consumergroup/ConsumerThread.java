package cn.mingyuan.kafkademos.consumergroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/25 15:52
 * @since jdk1.8
 */
public class ConsumerThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(Logger.class);
    private final String groupName;
    private final String threadName;
    private final String topic;
    private KafkaConsumer<String, String> consumer;
    private int maxCommitCount;

    public ConsumerThread(final String threadName, final String groupName, final String topic, final int maxCommitCount) {
        this.threadName = threadName;
        this.groupName = groupName;
        this.topic = topic;
        this.maxCommitCount = maxCommitCount;
        setName(threadName);
    }

    private void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");
        props.put("group.id", groupName);//不同的组，可以读取相同的消息，并且读取的offset互不影响
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("heartbeat.interval.ms", "3000");
        props.put("request.timeout.ms", "40000");//default 305000
//        props.put("send.buffer.bytes", "-1");//default 131072
//        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
        LOGGER.info(" init ok");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));
    }

    @Override
    public void run() {
        init();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.count() == 0) {
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info(String.format("partition=%s,offset=%s,key=%s,value=%s", record.partition(), record.offset(), record.key(), record.value()));
                System.out.println(getName() + "   " + String.format("partition=%s,offset=%s,key=%s,value=%s", record.partition(), record.offset(), record.key(), record.value()));
            }
            if (maxCommitCount-- > 0) {
                consumer.commitSync();
            }
        }
    }
}
