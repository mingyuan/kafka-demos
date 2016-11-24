package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/23 10:21
 * @since jdk1.8
 */
public class OffsetCommitDemo {

    public static void test(final String topic) {
        KafkaConsumer<String, String> consumer = ConsumerUtils.getManualCommitConsumer("test-commit331", "test.i313d");

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
        boolean set = true;
        while (true) {
            long position = consumer.position(new TopicPartition(topic, 0));
            System.out.println("partition 0 position=" + position);
            ConsumerRecords<String, String> records = consumer.poll(1000);//会自动增长offset
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
                long offset = recordList.get(recordList.size() - 1).offset();
                System.out.println(String.format("partition=%d, next-offset=%d", partition.partition(), offset));
                //存储到kafka之外，用于故障恢复
                store(partition.partition(), offset);
            }
            consumer.commitSync();
        }
    }

    private static void store(int partition, long offset) {
    }

    public static void main(String[] args) {
        System.out.println("version=3");
        String topic = "offset-test9999901";
//        ProducerDemo.generateMessage(topic);
        test(topic);
    }
}
