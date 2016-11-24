package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/23 10:21
 * @since jdk1.8
 */
public class OffsetCommitDemo {

    public static void test(final String topic) {
        KafkaConsumer<String, String> consumer = ConsumerUtils.getManualCommitConsumer("test-commit33", "test.i313d");

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Collection<TopicPartition> topicPartitions = new ArrayList<>(partitionInfos.size());
        partitionInfos.forEach(info -> {
            topicPartitions.add(new TopicPartition(topic, info.partition()));
        });

        consumer.assign(topicPartitions);
        for (TopicPartition topicPartition : topicPartitions) {
            consumer.seek(topicPartition, 0);
        }

        int count = 1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.count() == 0) {//没有数据就停止，因为是测试，所以不等待producer发数据
                continue;
            }
            System.out.println("---------------------------------------------------------------------------------");
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> recordList = records.records(partition);
                for (ConsumerRecord<String, String> consumerRecord : recordList) {
                    System.out.println(String.format("%d, partition= %d, offset= %d, key=%s, value=%s", count++, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value()));
                }
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(recordList.get(recordList.size() - 1).offset() + 1);
                System.out.println(String.format("commit-> partition=%d, next-offset=%d", partition.partition(), offsetAndMetadata.offset()));
                consumer.commitSync(Collections.singletonMap(new TopicPartition(topic, partition.partition()), offsetAndMetadata));
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("version=3");
        String topic = "offset-test999990";
        ProducerDemo.generateMessage(topic);
//        test(topic);
    }
}
