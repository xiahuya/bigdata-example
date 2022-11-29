package cn.xhjava;

import io.prometheus.client.Counter;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2022/10/9 0009
 */
public class CounterTest {

    public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("node4:9091");
        Counter counter = Counter
                .build()
                .name("data_realtime_ts")
                .labelNames("batch_10000")
                .help("这个指标是个什么意思呢?")
                .register();

        while (true) {
            counter.labels("op_ts").inc(1);
            gateway.push(counter, "test-20221114");
            Thread.sleep(3000);
        }

    }
}
