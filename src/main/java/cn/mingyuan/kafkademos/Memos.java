package cn.mingyuan.kafkademos;

/**
 * kafka的一些备忘，后后期转移到README.md中
 *
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/22 14:54
 * @since jdk1.8
 */
public class Memos {
    /**
     * 每一条消息被发送到broker时，会根据paritition规则选择被存储到哪一个partition。
     * 如果partition规则设置的合理，所有消息可以均匀分布到不同的partition里，这样就实现了水平扩展。
     * （如果一个topic对应一个文件，那这个文件所在的机器I/O将会成为这个topic的性能瓶颈，而partition解决了这个问题）。
     * 在创建topic时可以在$KAFKA_HOME/config/server.properties中指定这个partition的数量(如下所示)，
     * 当然也可以在topic创建之后去修改parition数量。
     * <p>
     * The default number of log partitions per topic.
     * More partitions allow greater parallelism for consumption,
     * but this will also result in more files across the brokers.<br/>
     * num.partitions=3
     * <p/>
     */
    public void howToModifyNumPartitionsForATopic() {

        // see  kafka-reassign-partitions.sh

    }

    /**
     * 同一个consumer group只有一个consumer能消费某一条消息
     * KafkaConsumer is not safe for multi-threaded access
     *
     * @see {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long)}
     */
    public void kafkaConsumerIsNotThreadSafe() {

    }

    /**
     * Kafka保证同一consumer group中只有一个consumer会消费某条消息，实际上，
     * Kafka保证的是稳定状态下每一个consumer实例只会消费某一个或多个特定partition的数据，
     * 而某个partition的数据只会被某一个特定的consumer实例所消费。
     * 这样设计的劣势是无法让同一个consumer group里的consumer均匀消费数据，
     * 优势是每个consumer不用都跟大量的broker通信，减少通信开销，同时也降低了分配难度，实现也更简单。
     * 另外，因为同一个partition里的数据是有序的，这种设计可以保证每个partition里的数据也是有序被消费。
     * <p>
     * 如果某consumer group中consumer数量少于partition数量，则至少有一个consumer会消费多个partition的数据，
     * 如果consumer的数量与partition数量相同，则正好一个consumer消费一个partition的数据，
     * 而如果consumer的数量多于partition的数量时，会有部分consumer无法消费该topic下任何一条消息。
     * 在这种策略下，每一个consumer或者broker的增加或者减少都会触发consumer rebalance。
     * 因为每个consumer只负责调整自己所消费的partition，为了保证整个consumer group的一致性，
     * 所以当一个consumer触发了rebalance时，该consumer group内的其它所有consumer也应该同时触发rebalance。
     */
    public void rebalance() {
        //rebalance 其实说的就是增加或减少consumer后，kafka集群如何为consumer指派partition
    }

    /**
     * <p> 消息Deliver guarantee</p>
     * Kafka如何确保消息在producer和consumer之间传输。有这么几种可能的delivery guarantee：
     * <ul>
     * <li>At most once 消息可能会丢，但绝不会重复传输</li>
     * <li>At least one 消息绝不会丢，但可能会重复传输</li>
     * <li>Exactly once 每条消息肯定会被传输一次且仅传输一次，很多时候这是用户所想要的。</li>
     * </ul>
     */
    public void deliveryGuarantee() {

    }

    /**
     * Kafka在Zookeeper中动态维护了一个ISR（in-sync replicas） set，
     * 这个set里的所有replica都跟上了leader，只有ISR里的成员才有被选为leader的可能。
     * For a topic with replication factor N, we will tolerate up to N-1 server failures without losing any records committed to the log.
     */
    public void redundancy() {
        //如果复制因子为N，那么最多容忍N-1个节点失效
        // http://kafka.apache.org/documentation#intro_guarantees
    }

    public static void main(String[] args) {
    }
}
