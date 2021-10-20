package com.clb.produce;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021/4/19
 */
public class ProduceTool {
    public static Properties getProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.114:9092");
        props.put("ack", "all");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");*/
        //添加拦截器
        /* List<String> interceptor = new ArrayList<>();
        interceptor.add("com.clb.interceptor.produce.MyProducerInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor);*/
        return props;
    }
}
