package com.clb.interceptor.produce;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/8/13
 * <p>
 * Produce拦截器
 */
public class MyProducerInterceptor implements ProducerInterceptor {
    /**
     * 需求:在消息发送之前,添加前置通知
     * 1.为消息增加头信息,封装改消息发送时间
     * 2.更新发送消息的数字段
     */


    //该方法会在消息发送之前被调用。
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return null;
    }

    //该方法会在消息成功提交或发送失败之后被调用
    //onAcknowledgement 的调用要早于 callback 的调用
    //onAcknowledgement 与 onSend不是同一个线程内被调用,所以在使用时需要注意线程安全问题
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> configs) {

    }
}
