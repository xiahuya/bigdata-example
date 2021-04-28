package com.clb.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
public class RandomProduceMsg {
    private static String topic = "flink_kafka_source";
    private static int count = 10000;
    private static int INDEX_T = 1;

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(ProduceTool.getProp());
        new RandomProduceMsg().start(topic, count, producer);
    }

    public void start(String topic, int count, KafkaProducer<String, String> producer) {
        int index = 0;
        while (true) {
            if (index >= INDEX_T) {
                break;
            }
            for (int i = 1; i <= count; i++) {
                ProducerRecord<String, String> msg = ProduceMsg.buildMsg(String.valueOf(i), topic);
                producer.send(msg);

                if (i % 100 == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
            index++;
        }
    }

}
