package com.clb.produce.Thread;

import com.clb.produce.ProduceMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-05-31
 * send ogg msg to kafka topic
 */

@Slf4j
public class ProductJsonMsg implements Runnable {

    private KafkaProducer<String, String> producer;
    private CountDownLatch cdl;
    private Integer msgCount;
    private String topic;
    private String tableName;

    public ProductJsonMsg(Integer msgCount, String topic, String tableName, CountDownLatch cdl, KafkaProducer<String, String> producer) {
        this.producer = producer;
        this.cdl = cdl;
        this.msgCount = msgCount;
        this.tableName = tableName;
        this.topic = topic;
    }

    @Override
    public void run() {
        for (int i = 1; i <= msgCount; i++) {
            ProducerRecord<String, String> msg = ProduceMsg.buildJsonMsg(String.valueOf(i), topic, tableName);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            producer.send(msg);
        }
        cdl.countDown();
        log.info("{} down~~~~", Thread.currentThread().getName());
    }
}
