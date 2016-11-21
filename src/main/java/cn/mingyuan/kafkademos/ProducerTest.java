package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) {
        //http://kafka.apache.org/0101/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
        PropertyConfigurator.configure("conf/log4j.properties");
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");//该地址是集群的子集，用来探测集群。参数中的域名没有ip地址好用，用域名有时会出现找不到leader的错误
//        props.put("bootstrap.servers", "vm1:9092,vm1:9093,vm1:9094");//该地址是集群的子集，用来探测集群。
        props.put("acks", "all");// 记录完整提交，最慢的但是最大可能的持久化
        props.put("retries", 3);// 请求失败重试的次数
        props.put("batch.size", 16384);// batch的大小
        props.put("linger.ms", 1);// 默认情况即使缓冲区有剩余的空间，也会立即发送请求，设置一段时间用来等待从而将缓冲区填的更多，单位为毫秒，producer发送数据会延迟1ms，可以减少发送到kafka服务器的请求数据
        props.put("buffer.memory", 33554432);// 提供给生产者缓冲内存总量
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化的方式，支持ByteArraySerializer或者StringSerializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);//thread-safe，建议一个jvm instance只启用一个Producer
        System.out.println("producer init ok,begine sending message");
        String topic = args[0];
        String identifier = " hello mingyuan";
        for (int i = 1; i <= 10; i++) {
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i) + identifier));
        }
        System.out.println("message sent!");

        producer.close();
        System.out.println("producer closed.");
    }
}
