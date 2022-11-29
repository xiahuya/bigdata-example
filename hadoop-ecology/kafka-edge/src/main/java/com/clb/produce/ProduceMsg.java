package com.clb.produce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
@Slf4j
public class ProduceMsg {

    private static String STUDENT_FORMAT = "%s,%s,%s,%s";
    private static List<String> partitionList = Arrays.asList("2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020");
    private static List<String> hospitalList = Arrays.asList("HID0101", "HID0102", "HID0103", "HID0104", "HID0105", "HID0106");
    private static List<String> sourceCodeList = Arrays.asList("1", "2", "3", "4", "5");
    private static List<String> formidList = Arrays.asList("549", "561", "531", "538", "540", "558", "559", "554", "307", "557", "541", "539", "542", "308", "287", "330", "556", "530", "560", "545", "552", "536", "543");
    private static Random random = new Random();

    private static Calendar calendar;


    static {
        calendar = Calendar.getInstance();
        calendar.setTime(new Date());
    }

    public static ProducerRecord<String, String> buildMsg(String rowkey, String topic) {
        long time = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
        String msg = String.format(STUDENT_FORMAT, rowkey, "XiaHu_" + rowkey, "man", time);
        log.info(msg);
        return new ProducerRecord(topic, msg);
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    /*private static String modle = "{\"table\":\"${table}\",\"op_type\":\"I\",\"op_ts\":\"${currentTime}\"," +
            "\"current_ts\":\"${currentTime}\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"]," +
            "\"after\":{\"id\":\"${rowkey}\",\"fk_id\":\"${fk_id}\",\"qfxh\":\"\",\"jdpj\":" + null + ",\"nioroa\":\"RTABPQ\",\"gwvz\":\"${SOURCE_CODE}\",\"joqtf\":\"${HOSPITAL}\"}}";
*/
    private static String modle = "{\"table\":\"${table}\",\"op_type\":\"I\",\"op_ts\":\"${currentTime}\"," +
            "\"current_ts\":\"${currentTime}\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"]," +
            "\"after\":{\"id\":\"${rowkey}\",\"fk_id\":\"${fk_id}\",\"qfxh\":\"\",\"jdpj\":" + null + ",\"nioroa\":\"RTABPQ\",\"gwvz\":\"${SOURCE_CODE}\",\"aaad\":\"${HOSPITAL}\"}}";

    public static ProducerRecord<String, String> buildOggMsg(String rowkey, String topic, String tableName) {
        String msg = modle.replace("${table}", tableName);
        msg = msg.replace("${rowkey}", rowkey);
        msg = msg.replace("${fk_id}", partitionList.get(random(random, 0, partitionList.size())));
        msg = msg.replace("${SOURCE_CODE}", sourceCodeList.get(random(random, 0, sourceCodeList.size())));
        msg = msg.replace("${HOSPITAL}", hospitalList.get(random(random, 0, hospitalList.size())));
        msg = msg.replace("${formid}", formidList.get(random(random, 0, formidList.size())));
        //calendar.add(Calendar.MINUTE, 1);//后退1分钟
        msg = msg.replace("${currentTime}", sdf.format(new Date()) + "123");

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


    private static String odsSinkModle = "{\"table\":\"${table}\",\"op_type\":\"I\",\"op_ts\":\"${currentTime}\"," +
            "\"current_ts\":\"${currentTime}\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"]," +
            "\"after\":{\"id\":\"${rowkey}\",\"age\":\"24\",\"name\":\"${SOURCE_CODE}\",\"sex\":\"男\"," +
            "\"address\":\"${address}\",\"phone\":\"1222xxxxxxxx\",\"salve\":\"${fk_id}\"}}";

    public static ProducerRecord<String, String> buildOdsSinkMsg(String rowkey, String topic, String tableName,StringBuffer address) {
        String msg = odsSinkModle.replace("${table}", tableName);
        msg = msg.replace("${rowkey}", rowkey);
        msg = msg.replace("${fk_id}", partitionList.get(random(random, 0, partitionList.size())));
        msg = msg.replace("${SOURCE_CODE}", sourceCodeList.get(random(random, 0, sourceCodeList.size())));
        msg = msg.replace("${address}", address);
        msg = msg.replace("${currentTime}", sdf.format(new Date()) + "123");
        return new ProducerRecord(topic, msg);
    }


}
