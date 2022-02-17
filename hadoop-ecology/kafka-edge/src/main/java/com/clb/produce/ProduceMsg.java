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
    private static List<String> formidList = Arrays.asList("549", "561", "531", "538", "540", "558", "559", "554", "307", "557", "541", "539", "542", "308", "287", "330", "556", "530", "560", "545", "552", "536", "543");
    private static Random random = new Random();

    public static ProducerRecord<String, String> buildMsg(String rowkey, String topic) {
        long time = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
        String msg = String.format(STUDENT_FORMAT, rowkey, "XiaHu_" + rowkey, "man", time);
        log.info(msg);
        return new ProducerRecord(topic, msg);
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    /*private static String modle = "{\n" +
            "\t\"table\": \"${table}\",\n" +
            "\t\"op_type\": \"U\",\n" +
            "\t\"op_ts\": \"2019-03-19 00:50:31.678015\",\n" +
            "\t\"current_ts\": \"${currentTime}\",\n" +
            "\t\"pos\": \"04172325706511144646\",\n" +
            "\t\"primary_keys\": [\n" +
            "\t\t\"id\"\n" +
            "\t],\n" +
            "\t\"before\": {\n" +
            "\t\t\"id\": \"${rowkey}\",\n" +
            "\t\t\"id1\": \"\",\n" +
            "\t\t\"id2\": null,\n" +
            "\t\t\"fk_id\": \"${fk_id}\",\n" +
            "\t\t\"qfxh1\": \"1.123\",\n" +
            "\t\t\"qfxh2\": \"\",\n" +
            "\t\t\"qfxh3\": null,\n" +
            "\t\t\"gwvz1\": \"2.234\",\n" +
            "\t\t\"gwvz2\": \"\",\n" +
            "\t\t\"gwvz3\": null,\n" +
            "\t\t\"joqtf1\": \"3.345\",\n" +
            "\t\t\"joqtf2\": \"\",\n" +
            "\t\t\"joqtf3\": null,\n" +
            "\t\t\"isdeleted\": \"0\",\n" +
            "\t\t\"lastupdatedttm\": \"${currentTime}\",\n" +
            "\t\t\"rowkey\": \"${rowkey}\"\n" +
            "\t},\n" +
            "\t\"after\": {\n" +
            "\t\t\"id\": \"${rowkey}\",\n" +
            "\t\t\"id1\": \"\",\n" +
            "\t\t\"id2\": null,\n" +
            "\t\t\"fk_id\": \"${fk_id}\",\n" +
            "\t\t\"qfxh1\": \"1.123\",\n" +
            "\t\t\"qfxh2\": \"\",\n" +
            "\t\t\"qfxh3\": null,\n" +
            "\t\t\"gwvz1\": \"2.234\",\n" +
            "\t\t\"gwvz2\": \"\",\n" +
            "\t\t\"gwvz3\": null,\n" +
            "\t\t\"joqtf1\": \"3.345\",\n" +
            "\t\t\"joqtf2\": \"\",\n" +
            "\t\t\"joqtf3\": null,\n" +
            "\t\t\"isdeleted\": \"0\",\n" +
            "\t\t\"lastupdatedttm\": \"${currentTime}\",\n" +
            "\t\t\"rowkey\": \"${rowkey}\"\n" +
            "\t}\n" +
            "}";*/


   /* private static String modle = "{\"table\":\"${table}\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\",\"current_ts\":\"${currentTime}\",\"pos\":\"04172325706511144646\"," +
            "\"primary_keys\":[\"rowkey\"],\"after\":{\"text5\":\"1\",\"text6\":\"2010\",\"text7\":\"3\",\"text8\":\"AFLWAI\"," +
            "\"formid\":\"${formid}\",\"field\":\"${rowkey}\",\"rowkey\":\"${rowkey}\"}}";*/


    private static String modle = "{\"table\":\"${table}\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\"," +
            "\"current_ts\":\"${currentTime}\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"]," +
            "\"after\":{\"id\":\"${rowkey}\",\"fk_id\":\"${fk_id}\",\"qfxh\":\"\",\"jdpj\":" + null + ",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\"}}";

    public static ProducerRecord<String, String> buildOggMsg(String rowkey, String topic, String tableName) {
        String msg = modle.replace("${table}", tableName);
        msg = msg.replace("${rowkey}", rowkey);
        msg = msg.replace("${fk_id}", partitionList.get(random(random, 0, partitionList.size())));
//        msg = msg.replace("${fk_id}", "1970");
//        msg = msg.replace("${partition}", partitionList.get(random(random, 0, partitionList.size())));
        msg = msg.replace("${formid}", formidList.get(random(random, 0, formidList.size())));
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


}
