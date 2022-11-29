package cn.xhjava;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.PushGateway;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2022/10/9 0009
 * 模拟：cache --> kafka --> flink --> hudi_commit_ts --> hudi_complete_ts
 * <p>
 * 折线图统计
 */
public class Test1 {
    private PushGateway gateway;

    @Before
    public void before() {
        gateway = new PushGateway("192.168.9.180:9091");
    }


    //============================================同一个指标,不同维度写入==================================================
    @Test
    public void test1() throws Exception {
        Gauge gauge = Gauge
                .build()
                .name("op_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();


        while (true) {
            gauge.labels("hid0101_cache_his_dhcapp_sqluser", "pa_adm", "2022-10-09_1") // 类型值
                    .set(5);
            gateway.pushAdd(gauge, "cache_datalake_data_realtime");
            Thread.sleep(3000);
            System.out.println("send ..");
        }

        /**
         * gateway.pushAdd() :一个job里面多个指标
         * gateway.push() : 一个job里面只有一个指标
         */
    }

    @Test
    public void test2() throws Exception {
        Gauge gauge2 = Gauge
                .build()
                .name("current_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();
        while (true) {
            gauge2.labels("hid0101_cache_his_dhcapp_sqluser", "pa_person", "2022-10-09_1") // 类型值
                    .set(10);
            gateway.pushAdd(gauge2, "cache_datalake_data_realtime");
            Thread.sleep(3000);
            System.out.println("send ..");
        }


    }


    //============================================不同指标,相同维度写入===================================================
    @Test
    public void test3() throws Exception {
        Gauge a = Gauge
                .build()
                .name("data_op_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();

        Gauge b = Gauge
                .build()
                .name("data_current_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();

        Gauge c = Gauge
                .build()
                .name("flink_load_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();

        Gauge d = Gauge
                .build()
                .name("hoodie_commit_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();

        Gauge e = Gauge
                .build()
                .name("hoodie_complate_ts") // 指标名称
                .labelNames("database", "table", "batch_id") // 类型名
                .help("no help")
                .register();


        for (int i = 1; i <= 1000; i++) {

            a.labels("hid0101_cache_his_dhcapp_sqluser", "pa_adm", String.format("2022-10-09_%s", i)) // 类型值
                    .set(0.5);
            gateway.pushAdd(a, "cache_datalake_data_realtime");

            Thread.sleep(30000);


            b.labels("hid0101_cache_his_dhcapp_sqluser", "pa_adm", String.format("2022-10-09_%s", i)) // 类型值
                    .set(1);
            gateway.pushAdd(b, "cache_datalake_data_realtime");

            Thread.sleep(30000);



            c.labels("hid0101_cache_his_dhcapp_sqluser", "pa_adm", String.format("2022-10-09_%s", i)) // 类型值
                    .set(1.5);
            gateway.pushAdd(c, "cache_datalake_data_realtime");

            Thread.sleep(30000);



            d.labels("hid0101_cache_his_dhcapp_sqluser", "pa_adm", String.format("2022-10-09_%s", i)) // 类型值
                    .set(2);
            gateway.pushAdd(d, "cache_datalake_data_realtime");

            Thread.sleep(30000);


            e.labels("hid0101_cache_his_dhcapp_sqluser", "pa_adm", String.format("2022-10-09_%s", i)) // 类型值
                    .set(10);
            gateway.pushAdd(e, "cache_datalake_data_realtime");

            Thread.sleep(60000);
        }
    }

}
