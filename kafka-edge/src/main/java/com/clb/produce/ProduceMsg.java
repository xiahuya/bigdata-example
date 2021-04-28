package com.clb.produce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
@Slf4j
public class ProduceMsg {

    private static String STUDENT_FORMAT = "%s,%s,%s,%s";

    public static ProducerRecord<String, String> buildMsg(String rowkey, String topic) {
        long time = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
        String msg = String.format(STUDENT_FORMAT, rowkey, "XiaHu_" + rowkey, "man", time);
        log.info(msg);
        return new ProducerRecord(topic, msg);
    }


}
