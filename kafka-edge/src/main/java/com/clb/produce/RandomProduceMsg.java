package com.clb.produce;

import com.clb.produce.Thread.InsertData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
@Slf4j
public class RandomProduceMsg {
    private static String topic = "kafka_join";
    private static List<String> topicList;
    private static int count = 300000;
    private static int INDEX_T = 30000;

    private static Random random;

    public static void main(String[] args) {
        topicList = Arrays.asList("kafka_join", "kafka_join2");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(ProduceTool.getProp());
        new RandomProduceMsg().start(topicList, count, producer);
        //new RandomProduceMsg().startOggMsg(topic, count, producer);
        producer.close();

    }

    static int tableCount = 45;

    public void startOggMsg(String topic, int count, KafkaProducer<String, String> producer) {
        String TABLE = "xh.testTable_";
        int size = 0;
        random = new Random();
        while (true) {
            int random = random(RandomProduceMsg.random, 1, 45);
            String tableName = String.format(TABLE, random);
            for (int j = 0; j <= 500; j++) {
                int i = random(RandomProduceMsg.random, 1, 100000000);
                ProducerRecord<String, String> msg = ProduceMsg.buildMsg(String.valueOf(i), "kafka_redis", tableName);
                producer.send(msg);
            }
            size = size + 500;
            log.info("增量数据: 500 ,共计数据: {}", size);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        for (int j = 1; j <= tableCount; j++) {
//            for (int i = 1; i <= count; i++) {
//                ProducerRecord<String, String> msg = ProduceMsg.buildMsg(String.valueOf(i), "kafka_redis", TABLE + j);
//                producer.send(msg);
//            }
//        }
    }


    public void start(List<String> topicList, int count, KafkaProducer<String, String> producer) {
        CountDownLatch cdl = new CountDownLatch(topicList.size());
        for (String topic : topicList) {
            new Thread(new InsertData(topic, count, INDEX_T, producer, cdl)).start();
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static int random(Random random, int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }

}
