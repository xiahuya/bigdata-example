package com.clb.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.clb.domain.*;
import com.clb.pojo.OggMsg;
import com.clb.util.UmsTool;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;

/**
 * @author Xiahu
 * @create 2020/12/9
 */
public class WormholeInterceptor implements ProducerInterceptor<String, String> {
    public static final String INCREMENT = "data_increment_data";
    public static final String SYSTEM_TYPE = "kafka";
    public static final String suffix = ".*.*.*";
    public static final String NAMESPACE_FORMAT = "%s.%s.%s.%s%s";
    public static final String INSTANCE = "ODS_HIS";
    private String namespace;
    private OggMsg oggMsg = null;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String key = parseMsgKey(record.topic(), record.value());
        String value = parseMsgValue(oggMsg, namespace);
        ProducerRecord<String, String> result = new ProducerRecord<>(
                record.topic(),
                record.partition(),
                record.timestamp(),
                key,
                value
        );
        return result;
    }

    public String parseMsgKey(String topic, String value) {
        oggMsg = JSONObject.parseObject(value, OggMsg.class);
        String tableName = oggMsg.getTable().replace(":", "\\.").toLowerCase();
        namespace = String.format(NAMESPACE_FORMAT, SYSTEM_TYPE, INSTANCE, topic, tableName, suffix);
        return INCREMENT + "." + namespace;
    }

    public String parseMsgValue(OggMsg oggMsg, String namespace) {
        /*UMS ums = UmsTool.getUMS(oggMsg, namespace);*/
        UMS result = new UMS();
        result.setProtocol(new Protocol("data_increment_data"));
        Schema schema = new Schema();
        List<Payload> payloadList = new ArrayList<>();
        schema.setNamespace(namespace);
        LinkedList<Fields> fields = new LinkedList<>();
        LinkedList<String> tuple = new LinkedList<>();
        Map<String, String> after = oggMsg.getAfter();
        List<String> pkList = oggMsg.getPrimary_keys();
        for (String pk : pkList) {
            //TODO 联合主键无法解析
            String s = after.get(pk);
        }
        fields.addLast(new Fields("ums_id_", "long", false));
        tuple.addLast("pk");
        fields.addLast(new Fields("ums_ts_", "datetime", false));
        tuple.addLast(oggMsg.getCurrent_ts());
        fields.addLast(new Fields("ums_op_", "string", false));
        tuple.addLast(oggMsg.getOp_type());
        Iterator<Map.Entry<String, String>> entryIterator = after.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            fields.addLast(new Fields(entry.getKey(), "string", false));
            tuple.addLast(entry.getValue());
        }
        schema.setFields(fields);
        payloadList.add(new Payload(tuple));
        result.setSchema(schema);
        result.setPayload(payloadList);
        return JSON.toJSONString(result, SerializerFeature.WriteMapNullValue);
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

    /*public static void main(String[] args) {
        WormholeInterceptor wormholeInterceptor = new WormholeInterceptor();
        String msg = "{\"table\":\"xh.test\",\"op_type\":\"I\",\"op_ts\":\"2019-03-19 00:50:31.678015\",\"current_ts\":\"2020-05-07T16:25:34.000290\",\"pos\":\"04172325706511144646\",\"primary_keys\":[\"id\"],\"after\":{\"id\":\"94\",\"fk_id\":\"2010\",\"qfxh\":\"94\",\"jdpj\":\"AFLWAI\",\"nioroa\":\"RTABPQ\",\"gwvz\":\"ZJRON\",\"joqtf\":\"VEZB\",\"isdeleted\":\"0\",\"lastupdatedttm\":\"2020-05-07 16:25:34.290\",\"rowkey\":\"94\"}}";
        *//*String namespace = "kafka.kafka01.datatopic.user.*.*.*";
        OggMsg oggMsg = JSONObject.parseObject(msg, OggMsg.class);
        String s = wormholeInterceptor.parseMsgValue(oggMsg, namespace);
        System.out.println(s);*//*
        String wormhole_ums = wormholeInterceptor.parseMsgKey("wormhole_ums", msg);
        System.out.println(wormhole_ums);
    }*/
}
