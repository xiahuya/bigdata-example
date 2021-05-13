package cn.xhjava.redis.high_concurrency;

import cn.xhjava.redis.Jedis_01;
import cn.xhjava.redis.pipline.JedisClusterPipeline;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/5/13
 * <p>
 * 多线程写入Redis集群
 */
@Slf4j
public class RedisClusterWriteThread implements Runnable {
    private JedisClusterPipeline pipeline = JedisClusterPipeline.pipelined(Jedis_01.getJedisCluster());
    private Map<String, String> data;
    private CountDownLatch cdl;

    public RedisClusterWriteThread(CountDownLatch cdl) {
        this.cdl = cdl;
    }

    @Override
    public void run() {
        log.info("开始请求Redis Cluster ");

        while (true) {
            this.data = RedisMain.dataKeyValue(10000);
            Iterator<Map.Entry<String, String>> iterator = data.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                pipeline.set(entry.getKey(), entry.getValue());
            }
            long startTime = System.currentTimeMillis();
            pipeline.sync();


            long endTime = System.currentTimeMillis();
            log.info("{}  写入数据  {} ,耗费时间 : {} s", Thread.currentThread().getName(), data.size(), (endTime - startTime) / 1000.0);
            if (false) {
                cdl.countDown();
            }
        }
    }
}
