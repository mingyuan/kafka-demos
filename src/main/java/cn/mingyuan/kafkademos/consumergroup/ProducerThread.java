package cn.mingyuan.kafkademos.consumergroup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/25 16:10
 * @since jdk1.8
 */
public class ProducerThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(Logger.class);

    private final String threadName;
    private final String topic;

    public ProducerThread(String threadName, final String topic) {
        this.threadName = threadName;
        this.topic = topic;
        setName(threadName);
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");//该地址是集群的子集，用来探测集群。参数中的域名没有ip地址好用，用域名有时会出现找不到leader的错误
        props.put("acks", "all");// 记录完整提交，最慢的但是最大可能的持久化
        props.put("retries", 1);// 请求失败重试的次数
        props.put("batch.size", 16384);// batch的大小
        props.put("linger.ms", 1);// 默认情况即使缓冲区有剩余的空间，也会立即发送请求，设置一段时间用来等待从而将缓冲区填的更多，单位为毫秒，producer发送数据会延迟1ms，可以减少发送到kafka服务器的请求数据
        props.put("buffer.memory", 33554432);// 提供给生产者缓冲内存总量
        props.put("max.block.ms", 3000);// The configuration controls how long KafkaProducer.send() and KafkaProducer.partitionsFor() will block.These methods can be blocked either because the buffer is full or metadata unavailable.Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化的方式，支持ByteArraySerializer或者StringSerializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        AtomicInteger seq = new AtomicInteger(1);
        int count = 1;
        while (true) {
            for (int i = 0; i < 5; i++) {
                String key = seq.get() + "";
                String value = seq.getAndIncrement() + "";
                producer.send(new ProducerRecord<String, String>(topic, key, value));
                LOGGER.info(String.format("批次=%s,key=%s", count, key));
            }
            count++;
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
