package com.clb.consumerByThread.v2;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2019/10/9
 * @since 1.0.0
 */
public class ConsumerRunnable implements Runnable {

    // 每个线程维护私有的KafkaConsumer实例
    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");        //本例使用自动提交位移
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));   // 本例使用分区副本自动分配策略
    }

    @Override
    public void run() {
        String msg = "线程名:[%s],Partition:[%s],Offset:[%s],Message:[%s]";
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);   // 本例使用200ms作为获取超时时间
            for (ConsumerRecord<String, String> consumerRecord : records) {
                // 这里面写处理消息的逻辑，本例中只是简单地打印消息
                System.out.println(String.format(msg, Thread.currentThread().getName(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value()));
            }
        }
    }
}