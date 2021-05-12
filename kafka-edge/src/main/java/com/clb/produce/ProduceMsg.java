package com.clb.produce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;

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

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static String modle = "{\"table\":\"${table}\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\",\"current_ts\":\"2020-05-07T16:25:34.000290\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"],\"after\":{\"id\":\"${rowkey}\",\"fk_id\":\"2010\",\"qfxh\":\"94\",\"jdpj\":\"AFLWAI\",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\",\"isdeleted\":\"0\",\"lastupdatedttm\":\"${currentTime}\",\"rowkey\":\"${rowkey}\"}}";

    public static ProducerRecord<String, String> buildMsg(String rowkey, String topic, String tableName) {
        String msg = modle.replace("${table}", tableName);
        msg = msg.replace("${rowkey}", rowkey);
        msg = msg.replace("${currentTime}", sdf.format(new Date()));
        log.debug(msg);
        return new ProducerRecord(topic, msg);
    }


}
