package com.clb.consumerByThread.v1;

/**
 * @author Xiahu
 * @create 2019/10/8
 * @since 1.0.0
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Worker implements Runnable {

    private ConsumerRecord<String, String> consumerRecord;

    public Worker(ConsumerRecord record) {
        this.consumerRecord = record;
    }

    @Override
    public void run() {
        // 这里写你的消息处理逻辑，本例中只是简单地打印消息
        String msg = "线程名:[%s],Partition:[%s],Offset:[%s],Message:[%s]";
        System.out.println(String.format(msg,Thread.currentThread().getName(),consumerRecord.partition(),consumerRecord.offset(),consumerRecord.value()));
    }
}