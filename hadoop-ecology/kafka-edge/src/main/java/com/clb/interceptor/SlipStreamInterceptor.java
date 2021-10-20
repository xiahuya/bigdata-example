package com.clb.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.clb.domain.OggMsg;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;

/**
 * properties.put("interceptor.classes", "com.clb.interceptor.SlipStreamInterceptor");
 *
 * @author Xiahu
 * @create 2020/12/9
 */
public class SlipStreamInterceptor implements ProducerInterceptor<byte[], byte[]> {
    public static final String DELIMITER = "^";
    public static final String ISDELETED = "isdeleted";
    public static final String LASTUPDATEDTTM = "lastupdatedttm";
    public static final String ROWKEY = "rowkey";

    public static final String SPLIT = "_";


    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
        String msg = new String(record.value(), 0, record.value().length);
        String value = parseMsgValue(msg);
        ProducerRecord<byte[], byte[]> result = new ProducerRecord<>(
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                value.getBytes()
        );
        return result;
    }


    public String parseMsgValue(String msg) {
        OggMsg oggMsg = JSON.parseObject(msg, OggMsg.class);
        String result = null;

        //根据op_type 走不同的逻辑
        if (oggMsg.getOp_type().equals("I") || oggMsg.getOp_type().equals("U")) {
            String rowkey = parseRowkey(oggMsg.getPrimary_keys(), oggMsg.getAfter());
            result = parseColumn(oggMsg.getAfter(), "0", oggMsg.getCurrent_ts().replace("T", " "), rowkey);
        } else {
            String rowkey = parseRowkey(oggMsg.getPrimary_keys(), oggMsg.getBefore());
            result = parseColumn(oggMsg.getBefore(), "1", oggMsg.getCurrent_ts().replace("T", " "), rowkey);
        }
        return result;
    }


    //解析rowkey,联合主键使用 _ 连接
    private static String parseRowkey(LinkedList<String> primaryKeys, LinkedHashMap<String, String> after) {
        StringBuffer sb = new StringBuffer();
        if (primaryKeys.size() <= 1) {
            String primaryKey = primaryKeys.getFirst();
            if (after.containsKey(primaryKey)) {
                sb.append(after.get(primaryKey));
            }
        } else {
            for (String primaryKey : primaryKeys) {
                if (after.containsKey(primaryKey)) {
                    sb.append(after.get(primaryKey)).append(SPLIT);
                }
            }
            if (sb.toString().endsWith(SPLIT)) {
                sb.deleteCharAt(sb.lastIndexOf("_"));
            }
        }

        return sb.toString();
    }

    private static String parseColumn(LinkedHashMap<String, String> after,
                                      String isDeleted,
                                      String lastupdatedttm,
                                      String rowkey) {
        JSONObject jsonObject = new JSONObject();

        Set<Map.Entry<String, String>> entrySet = after.entrySet();
        Iterator<Map.Entry<String, String>> entryIterator = entrySet.iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            jsonObject.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        jsonObject.put(ISDELETED, isDeleted);
        jsonObject.put(LASTUPDATEDTTM, lastupdatedttm);
        jsonObject.put(ROWKEY, rowkey);
        return JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue);
    }


    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    public static void main(String[] args) {
        SlipStreamInterceptor wormholeInterceptor = new SlipStreamInterceptor();
        //String msg = "{\"table\":\"xh.test\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\",\"current_ts\":\"2020-05-07T16:25:34.000290\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"],\"after\":{\"id\":\"94\",\"fk_id\":\"2010\",\"qfxh\":\"94\",\"jdpj\":\"AFLWAI\",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\"}}";
        //String msg = "{\"table\":\"xh.test\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\",\"current_ts\":\"2020-05-07T16:25:34.000290\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\",\"fk_id\"],\"after\":{\"id\":\"94\",\"fk_id\":\"2010\",\"qfxh\":NULL,\"jdpj\":\"AFLWAI\",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\"}}";
        String msg = "{\"table\":\"HID0101_CACHE_XDCS_PACS_HJ.MERGE_TEST1\",\"op_type\":\"U\",\"op_ts\":\"2021-03-18 18:26:08.501566\",\"current_ts\":\"2021-03-19T02:26:12.966000\",\"pos\":\"00000000000001991122\",\"primary_keys\":[\"ID\",\"AGE\"],\"before\":{\"ID\":307,\"NAME\":\"aa\",\"AGE\":21,\"BIRTHDAY\":\"2021-01-18 17:48:28\",\"SEX\":1,\"CREATE_TIME\":\"2021-01-18 17:48:28\"},\"after\":{\"ID\":307,\"NAME\":\"aa\",\"AGE\":21,\"BIRTHDAY\":\"2021-01-18 17:48:28\",\"SEX\":1,\"CREATE_TIME\":\"2021-01-18 17:48:28\"}}";

        String value = wormholeInterceptor.parseMsgValue(msg);
        System.out.println(value.toString());

        /*String namespace = "kafka.kafka01.datatopic.user.*.*.*";
        OggMsg oggMsg = JSONObject.parseObject(msg, OggMsg.class);
        String s = wormholeInterceptor.parseMsgValue(oggMsg, namespace);
        System.out.println(s);*/
        /*String wormhole_ums = wormholeInterceptor.parseMsgKey("wormhole_ums", msg);
        System.out.println(wormhole_ums);*/
    }
}
