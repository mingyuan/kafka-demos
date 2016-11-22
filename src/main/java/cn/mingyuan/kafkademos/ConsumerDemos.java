package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.*;

public class ConsumerDemos {
    private static final Logger LOGGER = Logger.getLogger(ConsumerDemos.class);

    /**
     * auto offset committing<br>
     */
    private static void autoOffsetCommitting(final String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");
        props.put("group.id", "test3");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        //如果consumer崩溃或者无法发送心跳，在超过session.timeout.ms时间之后，这个consumer被认为挂掉，它持有的partition将会被重新分配
        props.put("session.timeout.ms", "30000");
        //earliest: 从最早数据读取，对相同的group.id只生效一次，第一次执行完毕后，再运行程序也不会获取到最早数据
        //latest: 最新数据开始读取，这个参数设置之后，程序就开始读取程序启动之后接收到的消息，以前的数据不再处理了
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//等待100毫秒，有没有数据本方法都将返回，若无数据，返回一个size=0的结果，不会为null
            int count = 1;
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%d  partion= %s,offset = %d, key = %s, value = %s%n", count++, record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 手动设置offset方式读取数据。use-case：获取一批数据-处理-标记完成（commitSync）
     * <br>
     * 若配合consumer.commitSync(offsetAndMetadataMap);方法，将offset设置为0，那么就可以从头读取到所有的数据
     */
    private static void manualOffsetControl(final String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");
        props.put("group.id", "test5");//不同的组，可以读取相同的消息，并且读取的offset互不影响
        //禁用自动提交
        props.put("enable.auto.commit", "false");
        //如果consumer崩溃或者无法发送心跳，在超过session.timeout.ms时间之后，这个consumer被认为挂掉，它持有的partition将会被重新分配
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));//订阅topic
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);//获取topic的分区信息

        //设置所有分区的offset为从0开始
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(0);
            offsetAndMetadataMap.put(topicPartition, offsetAndMetadata);
        }
        consumer.commitSync(offsetAndMetadataMap);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);//设置为LONG.MAX_VALUE目的是一直等待，直到获取到数据
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    int count = 1;
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        LOGGER.info(String.format("%d  partition= %d, offset = %s, key=%s, value=%s", count++, record.partition(), record.offset(), record.key(), record.value()));
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 一次性读取指定topic的所有分区里面的所有数据
     */
    public static void getALL(final String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
        if (partitionsFor != null && partitionsFor.size() > 0) {
            partitionsFor.forEach(e -> {
                LOGGER.info(String.format("--------------- partition= %d ----------------", e.partition()));
                TopicPartition topicPartition = new TopicPartition(topic, e.partition());
                consumer.assign(Arrays.asList(topicPartition));//为consumer分配分区
                consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(0)));//设置从offset=0开始读取
                ConsumerRecords<String, String> records = consumer.poll(Integer.MAX_VALUE);//获取所有数据
                int count = 1;
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info(String.format("%d partition=%s offset=%s  key=%s value=%s", count++, record.partition(), record.offset(), record.key(), record.value()));
                }
            });
        }

        consumer.close();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        final String topic = "my-topic112300";
//        autoOffsetCommitting(topic);

//        manualOffsetControl(topic);
        getALL(topic);
        //System.out.printf("a-%ne-b");
    }
}
