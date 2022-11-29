package cn.xhjava;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Xiahu
 * @create 2022/10/9 0009
 */
@Slf4j
public class GaugeTest2 {
    /*public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("192.168.9.180:9091");
        Gauge gauge = Gauge
                .build()
                .name("pa_adm")
                .labelNames("batch_id_1001")
                .help("no help")
                .register();

        while (true) {
            gauge.labels("current_ts").set(2);
            gateway.push(gauge, "dl");
            Thread.sleep(3000);
        }
    }*/


    public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("192.168.9.180:9091");
        Gauge gauge = Gauge
                .build()
                .name("op_ts") // 指标名称
                .labelNames("database","table","batch_id") // 类型名
                .help("no help")
                .register();

        while (true) {
            gauge.labels("hid0101_cache_his_dhcapp_sqluser","pa_adm","2022-10-09_2") // 类型值
                    .set(10);
            gateway.push(gauge, "cache_datalake_data_realtime");
            Thread.sleep(3000);
            System.out.println("send ..");
        }
    }
}
