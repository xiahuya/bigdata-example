package com.clb.consumerByThread.v1;

/**
 * @author Xiahu
 * @create 2019/10/8
 * @since 1.0.0
 */
public class Main {

    public static void main(String[] args) {
        String brokerList = "node2:9092,node3:9092,node4:9092";
        String groupId = "default";
        String topic = "test_kafkaspeed";
        int workerNum = 3;

        ConsumerHandler consumers = new ConsumerHandler(brokerList, groupId, topic);
        consumers.execute(workerNum);




        try {
            Thread.sleep(1000000);
        } catch (InterruptedException ignored) {
        }
        consumers.shutdown();
    }
}