package cn.xhjava;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Random;

/**
 * @author Xiahu
 * @create 2020/12/18
 */
public class PrometheusConn {
    public static void main(String[] args) throws Exception {
        PushGateway gateway = new PushGateway("192.168.0.115:9091");
        Counter counter = Counter
                .build()
                .name("blog_visit")
                .labelNames("blog_id")
                .help("xiahu")
                .register();

        Gauge gauge = Gauge.build().name("blog_fans").help("xiahu").register();
        Random rnd = new Random();

        //粉丝数先预设50
        gauge.inc(-1);
        counter.labels("count").inc(2);
        while (true) {
            //随机生成1个blogId
//            int blogId = rnd.nextInt(100000);
            //该blogId的访问量+1
//            counter.labels(blogId + "").inc();
//            //模拟粉丝数的变化
//            if (blogId % 2 == 0) {
//                gauge.inc();
//            } else {
//                gauge.dec();
//            }
            //利用网关采集数据
            gateway.push(counter, "job-counter-test");
            gateway.push(gauge, "job-gauge-test");

            //辅助输出日志
            //System.out.println("blogId:" + blogId);
            Thread.sleep(5000);
        }
    }
}


