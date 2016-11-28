package cn.mingyuan.kafkademos.consumergroup;

import org.apache.log4j.PropertyConfigurator;

import java.util.concurrent.TimeUnit;

/**
 * 同一个group中的不同consumer回一起消费topic中的消息<br/>
 * 当一个consumer失败后，其它的consumer会接管失败的consumer所对应的partition<br/>
 * 假设t1失败后，t2接管t1对应的partition 2，那么t2将会从t1最后一次commit的offset开始读取数据<br/>
 * 因为存在t1读取了消息，但没有commit的情况，因此t2可能重复消费t1已经消费过的消息<br/>
 * <p>
 * ProducerThread：模拟消息生产者<br/>
 * ConsumerThread t1模拟上述的t1<br/>
 * ConsumerThread t2模拟上述的t2<br/>
 * t1提交3次offset之后便不再提交，并且t1运行30秒后退出,t2接管t1的partition<br>
 *
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/25 16:19
 * @since jdk1.8
 */
public class Driver {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws InterruptedException {
        PropertyConfigurator.configure("conf/log4j.properties");
        String group = args[0];
        String topic = "this-is-test-" + System.currentTimeMillis();
        new ProducerThread("producer", topic).start();

        TimeUnit.SECONDS.sleep(10);

        ConsumerThread t1 = new ConsumerThread("t-1", group, topic, 3);
        ConsumerThread t2 = new ConsumerThread("t-2", group, topic, 10000000);
        t1.start();
        t2.start();

        System.out.println("程序启动完毕");
        TimeUnit.SECONDS.sleep(30);
        System.out.println("停止线程t-1");
        t1.stop();
        System.out.println("t-1停止");
    }
}
