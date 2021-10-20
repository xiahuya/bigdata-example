package com.clb.produce.Thread;

import com.clb.produce.ProduceMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-05-31
 * send ogg msg to kafka topic
 */

@Slf4j
public class ProductOggMsg implements Runnable {

    private KafkaProducer<String, String> producer;
    private CountDownLatch cdl;
    private Integer msgCount;
    private String topic;
    private String tableName;

    public ProductOggMsg(Integer msgCount, String topic, String tableName, CountDownLatch cdl, KafkaProducer<String, String> producer) {
        this.producer = producer;
        this.cdl = cdl;
        this.msgCount = msgCount;
        this.tableName = tableName;
        this.topic = topic;
    }

    @Override
    public void run() {
        for (int i = 1; i <= msgCount; i++) {
            ProducerRecord<String, String> msg = ProduceMsg.buildOggMsg(String.valueOf(i), topic, tableName);
            producer.send(msg);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        for (int i = 1; i <= msgCount; i++) {
//            ProducerRecord<String, String> msg = ProduceMsg.buildOggMsg(String.valueOf(i), topic, tableName);
//            producer.send(msg);
////            try {
////                Thread.sleep(1000);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//        }
        cdl.countDown();
        log.info("{} down~~~~", Thread.currentThread().getName());
    }
}
