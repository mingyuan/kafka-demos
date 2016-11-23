package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/23 10:21
 * @since jdk1.8
 */
public class ConsumerUtils {

    public static <T, V> KafkaConsumer<T, V> getManualCommitConsumer(String groupName, String clinetId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.151.179:9092");
        props.put("group.id", groupName);//不同的组，可以读取相同的消息，并且读取的offset互不影响
        props.put("client.id",clinetId);
        //禁用自动提交
        props.put("enable.auto.commit", "false");
        //如果consumer崩溃或者无法发送心跳，在超过session.timeout.ms时间之后，这个consumer被认为挂掉，它持有的partition将会被重新分配
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<T, V> consumer = new KafkaConsumer<T, V>(props);
        return consumer;
    }
}
