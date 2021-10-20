package com.clb.produce;

import com.clb.produce.Thread.InsertData;
import com.clb.produce.Thread.ProductJsonMsg;
import com.clb.produce.Thread.ProductOggMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
@Slf4j
public class RandomProduceMsg {
    private static List<String> topicList;
    private static int count = 1000;
    private static int INDEX_T = 1;


    public static void main(String[] args) {
        topicList = Arrays.asList("kafka_join", "kafka_join2", "kafka_join3");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(ProduceTool.getProp());
//        new RandomProduceMsg().start(topicList, count, producer);
        new RandomProduceMsg().startOggMsg(count, producer);
//        new RandomProduceMsg().startJsonMsg(count, producer);
    }

    public void startJsonMsg(int count, KafkaProducer<String, String> producer) {
        long startTime = System.currentTimeMillis();
        String TABLE = "xh.test_";
        String topic = "xh_cow";
        CountDownLatch cdl = new CountDownLatch(1);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int j = 1; j <= INDEX_T; j++) {
            executorService.submit(new ProductJsonMsg(count, topic, TABLE + j, cdl, producer));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cdl.await();
                    executorService.shutdown();
                    long endTime = System.currentTimeMillis();
                    log.info("总共写入数据: {} ,耗费时间: {} s", count + INDEX_T, (endTime - startTime) / 1000.0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    producer.close();
                }
            }
        }).start();
    }


    public void startOggMsg(int count, KafkaProducer<String, String> producer) {
        long startTime = System.currentTimeMillis();
//        String TABLE = "hid0101_his_cache_xh.test_";
        String TABLE = "hid0101_cache_his_dhcapp_nemr.test_";
//        String TABLE = "nuwa_consumer_oracle_sink.test_";
//        String topic = "flink_stream_sql";
//        String topic = "kafka_to_oracle";
//        String topic = "kafka_to_hbase";
//        String topic = "kafka_to_hdfs";
//        String topic = "kafka_to_mysql";
//        String topic = "dip_data_pre_process";
        String topic = "unnest-join-table-pre";
        //String topic = "nuwa_hudi";
        //String topic = "flink_sql_kafkasource";
        CountDownLatch cdl = new CountDownLatch(INDEX_T);
        ExecutorService executorService = Executors.newFixedThreadPool(INDEX_T);
        for (int j = 1; j <= INDEX_T; j++) {
            executorService.submit(new ProductOggMsg(count, topic, TABLE + j, cdl, producer));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cdl.await();
                    executorService.shutdown();
                    long endTime = System.currentTimeMillis();
                    log.info("总共写入数据: {} ,耗费时间: {} s", count + INDEX_T, (endTime - startTime) / 1000.0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    producer.close();
                }
            }
        }).start();
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
        } finally {
            producer.close();
        }
    }

    public static int random(Random random, int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }

}
