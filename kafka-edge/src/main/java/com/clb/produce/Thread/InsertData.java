package com.clb.produce.Thread;

import com.clb.produce.ProduceMsg;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/5/12
 */
public class InsertData implements Runnable {
    String topic;
    int count;
    int INDEX_T;
    KafkaProducer<String, String> producer;
    CountDownLatch cdl;

    public InsertData(String topic, int count, int INDEX_T, KafkaProducer<String, String> producer, CountDownLatch cdl) {
        this.topic = topic;
        this.count = count;
        this.INDEX_T = INDEX_T;
        this.producer = producer;
        this.cdl = cdl;
    }


    @Override
    public void run() {
        int index = 0;
        while (true) {
            if (index >= INDEX_T) {
                break;
            }
            for (int i = 1; i <= count; i++) {
                ProducerRecord<String, String> msg = ProduceMsg.buildMsg(String.valueOf(i), topic);
                producer.send(msg);

//                if (i % 1000 == 0) {
//                    try {
//                        Thread.sleep(800);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
            }
            index++;
        }
        cdl.countDown();
    }
}
