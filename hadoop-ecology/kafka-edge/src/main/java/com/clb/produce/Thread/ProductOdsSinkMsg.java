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
public class ProductOdsSinkMsg implements Runnable {

    private KafkaProducer<String, String> producer;
    private CountDownLatch cdl;
    private Integer msgCount;
    private String topic;
    private String tableName;

    private StringBuffer address;

    public ProductOdsSinkMsg(Integer msgCount, String topic, String tableName, CountDownLatch cdl, KafkaProducer<String, String> producer) {
        this.producer = producer;
        this.cdl = cdl;
        this.msgCount = msgCount;
        this.tableName = tableName;
        this.topic = topic;
        address = new StringBuffer();

        for (int i = 0; i <= 1; i++) {
            address.append("中国是世界上历史最悠久的国家之一。中国各族人民共同创造了光辉灿烂的文化，具有光荣的革命传统。一八四〇年以后，封建的中国逐渐变成半殖民地、半封建的国家。中国人民为国家独立、民族解放和民主自由进行了前仆后继的英勇奋斗。\n");
        }

    }

    @Override
    public void run() {
        for (int i = 1; i <= msgCount; i++) {
            ProducerRecord<String, String> msg = ProduceMsg.buildOdsSinkMsg(String.valueOf(i), topic, tableName,address);
            producer.send(msg);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cdl.countDown();
        log.info("{} down~~~~", Thread.currentThread().getName());
    }
}
