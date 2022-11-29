package cn.xhjava;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Random;

/**
 * @author Xiahu
 * @create 2022/10/9 0009
 */
@Slf4j
public class GaugeTest1 {
    /*public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("192.168.9.180:9091");
        Gauge gauge = Gauge
                .build()
                .name("pa_adm") // 指标名称
                .labelNames("batch_id_1001") // 类型名
                .help("no help")
                .register();

        while (true) {
            gauge.labels("op_ts") // 类型值
                    .set(1);
            gateway.push(gauge, "cache_datalake_data_realtime");
            Thread.sleep(3000);
        }
    }*/


    public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("localhost:8080");
        Gauge gauge = Gauge
                .build()
                .name("kafka_pending_time") // 指标名称
                .labelNames("data_link","topic","batch_id") // 类型名
                .help("no help")
                .register();


        while (true) {
            gauge.labels("dl","list","20221009_1") // 类型值
                    .set(500);
            gateway.pushAdd(gauge, "hid0102_cache_his_sqluser.test_1");
            Thread.sleep(10000);
            System.out.println("send ..");
        }
    }

}
