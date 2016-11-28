package cn.mingyuan.kafkademos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/24 17:59
 * @since jdk1.8
 */
public class OffsetSubmitTest extends Thread {
    private String groupName;
    private String clientId;
    private String topic;
    private boolean second;
    private KafkaConsumer<String, String> consumer;


    public OffsetSubmitTest(final String groupName, final String clientId, final String topic, final boolean second) {
        this.groupName = groupName;
        this.clientId = clientId;
        this.topic = topic;
        this.second = second;
        setName(groupName + "--" + clientId);
        initConsumer();
    }

    private void initConsumer() {
        consumer = ConsumerUtils.getManualCommitConsumer(this.groupName, this.clientId);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        if (second) {
            try {
                TimeUnit.SECONDS.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(getName() + " begin process");
        int count = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
//            System.out.println(getName() + " records got size=" + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("thread=%s, partition=%s,offset=%s,key=%s,value=%s", getName(), record.partition(), record.offset(), record.key(), record.value()));
            }
//            consumer.commitSync();
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "googleearch-1";// + System.currentTimeMillis();
        System.out.println(topic);
        String groupName = "group-1";
        String clientName = "client-"+System.currentTimeMillis()+"-";
        for (int i = 1; i < 2; i++) {
            OffsetSubmitTest t1 = new OffsetSubmitTest(groupName, clientName + i, topic, false);
            t1.start();
        }
//        TimeUnit.SECONDS.sleep(10);
//        for (int i = 1; i < 11; i++) {
//            ProducerDemo.generateMessage(topic, String.valueOf(i));
//            TimeUnit.SECONDS.sleep(5);
//        }
    }
}
