package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * 同一个key，分配到同一个分区中<b>
 * 默认使用DefaultPartitioner进行分区
 *
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/22 18:29
 * @see {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner#partition(String, Object, byte[], Object, byte[], Cluster)}
 * @since jdk1.8
 */
public class SameKeyPartitionDemo {

    public static void produceMessage(final String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092,172.16.151.179:9093,172.16.151.179:9094");//该地址是集群的子集，用来探测集群。参数中的域名没有ip地址好用，用域名有时会出现找不到leader的错误
//        props.put("bootstrap.servers", "vm1:9092,vm1:9093,vm1:9094");//该地址是集群的子集，用来探测集群。
        props.put("acks", "all");// 记录完整提交，最慢的但是最大可能的持久化
        props.put("retries", 1);// 请求失败重试的次数
        props.put("batch.size", 16384);// batch的大小
        props.put("linger.ms", 1);// 默认情况即使缓冲区有剩余的空间，也会立即发送请求，设置一段时间用来等待从而将缓冲区填的更多，单位为毫秒，producer发送数据会延迟1ms，可以减少发送到kafka服务器的请求数据
        props.put("buffer.memory", 33554432);// 提供给生产者缓冲内存总量
        props.put("max.block.ms", 3000);// The configuration controls how long KafkaProducer.send() and KafkaProducer.partitionsFor() will block.These methods can be blocked either because the buffer is full or metadata unavailable.Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化的方式，支持ByteArraySerializer或者StringSerializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topic, "generateMessage-key", "generateMessage-value-" + i));
        }

        producer.close();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        final String topic = "test-same-key";
        produceMessage(topic);
        System.out.println("----------------------------------------------------------------");
        ConsumerDemos.getALL(topic);
    }
}
