package com.clb.produce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
@Slf4j
public class ProduceMsg {

    private static String STUDENT_FORMAT = "%s,%s,%s,%s";
    private static List<String> partitionList = Arrays.asList("2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020");
    private static Random random = new Random();

    public static ProducerRecord<String, String> buildMsg(String rowkey, String topic) {
        long time = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
        String msg = String.format(STUDENT_FORMAT, rowkey, "XiaHu_" + rowkey, "man", time);
        log.info(msg);
        return new ProducerRecord(topic, msg);
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static String modle = "{\"table\":\"${table}\",\"op_type\":\"U\",\"op_ts\":\"2019-03-19 00:50:31.678015\"," +
            "\"current_ts\":\"${currentTime}\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\",\"fk_id\"]," +
            "\"after\":{\"id\":\"${rowkey}\",\"fk_id\":\"${rowkey}\",\"qfxh\":\"\",\"jdpj\":" + null + ",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\"," +
            "\"isdeleted\":\"0\",\"lastupdatedttm\":\"${currentTime}\",\"rowkey\":\"${rowkey}\"}}";

    public static ProducerRecord<String, String> buildOggMsg(String rowkey, String topic, String tableName) {
        String msg = modle.replace("${table}", tableName);
        msg = msg.replace("${rowkey}", rowkey);
        msg = msg.replace("${partition}", partitionList.get(random(random, 0, partitionList.size())));
        msg = msg.replace("${currentTime}", sdf.format(new Date()));
        log.info(msg);
        return new ProducerRecord(topic, msg);
    }

    private static String modle2 = "{\"id\": \"${rowkey}\",\"fk_id\": \"${partition}\",\"qfxh\": \"94\",\"jdpj\": \"AFLWAI\",\"nioroa\": \"RTABPQ\"," +
            "\"gwvz\": \"ZJRON\",\"joqtf\": \"VEZB\",\"isdeleted\": \"0\",\"lastupdatedttm\": \"${currentTime}\",\"rowkey\": \"${rowkey}\"}";

    public static ProducerRecord<String, String> buildJsonMsg(String rowkey, String topic, String tableName) {
        String msg = modle2.replace("${table}", tableName);
        msg = msg.replace("${rowkey}", rowkey);
        msg = msg.replace("${partition}", partitionList.get(random(random, 0, partitionList.size())));
        msg = msg.replace("${currentTime}", sdf.format(new Date()));
        log.info(msg);
        return new ProducerRecord(topic, msg);
    }


    public static int random(Random random, int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }


}
