package com.clb.consumerByThread.v2;

/**
 * @author Xiahu
 * @create 2019/10/9
 * @since 1.0.0
 */
public class ConsumerMain {

    public static void main(String[] args) {
        String brokerList = "node2:9092,node3:9092,node4:9092";
        String groupId = "default";
        String topic = "test_kafkaspeed";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}