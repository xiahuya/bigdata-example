package cn.xhjava;

import io.prometheus.client.Counter;
import io.prometheus.client.exporter.PushGateway;

/**
 * @author Xiahu
 * @create 2022/10/9 0009
 */
public class CounterTest2 {

    public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("192.168.9.180:9091");
        Counter counter = Counter
                .build()
                .name("data_realtime_ts")
                .labelNames("batch_10000")
                .help("no help")
                .register();

        while (true) {
            counter.labels("curren_ts").inc(2);
            gateway.push(counter, "job_batch_10000");
            Thread.sleep(3000);
        }
    }
}
