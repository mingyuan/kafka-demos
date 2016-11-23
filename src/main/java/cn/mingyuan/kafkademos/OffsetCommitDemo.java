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
        KafkaConsumer<String, String> consumer = ConsumerUtils.getManualCommitConsumer("test-commit", "test.id");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
        }));
//        consumer.subscribe(Arrays.asList(topic));
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Collection<TopicPartition> topicPartitions = new ArrayList<>(partitionInfos.size());
        partitionInfos.forEach(info -> {
            topicPartitions.add(new TopicPartition(topic, info.partition()));
        });

        consumer.assign(topicPartitions);

        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
            long position = consumer.position(topicPartition);
            System.out.println("partition=" + partitionInfo.partition() + " offset=" + position);
            map.put(topicPartition, new OffsetAndMetadata(0));//指定下一个读取位置（从0开始）
        }
        consumer.commitSync(map);

        int count = 1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if(records.count()==0){//没有数据就停止，因为是测试，所以不等待producer发数据
                break;
            }
            System.out.println("---------------------------------------------------------------------------------");
            Set<TopicPartition> partitions = records.partitions();
            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> recordList = records.records(partition);
                for (ConsumerRecord<String, String> consumerRecord : recordList) {
                    System.out.println(String.format("%d, partition= %d, offset= %d, key=%s, value=%s", count++, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value()));
                }
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(recordList.get(recordList.size() - 1).offset() + 1);
                System.out.println(String.format("commit-> partition=%d, next-offset=%d",partition.partition(),offsetAndMetadata.offset()));
                consumer.commitSync(Collections.singletonMap(new TopicPartition(topic, partition.partition()),offsetAndMetadata ));
            }
        }
    }

    public static void main(String[] args) {
        String topic = "offset-test100";
//        ProducerDemo.generateMessage(new String[]{topic});
        test(topic);
    }
}
