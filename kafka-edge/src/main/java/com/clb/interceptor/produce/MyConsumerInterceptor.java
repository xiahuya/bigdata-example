package com.clb.interceptor.produce;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/8/13
 * <p>
 * consumer 拦截器
 */
public class MyConsumerInterceptor implements ConsumerInterceptor<String, String> {


    //该方法在消息返回给 Consumer 程序之前调用.
    //也就是说在开始正式处理消息之前，拦截器会先拦一道，搞一些事情，之后再返回给你
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        return null;
    }


    //在提交位移之后调用该方法。通常你可以在该方法中做一些记账类的动作，比如打日志等。
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}