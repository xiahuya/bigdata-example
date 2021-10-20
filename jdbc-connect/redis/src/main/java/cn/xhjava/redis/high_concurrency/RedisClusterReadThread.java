package cn.xhjava.redis.high_concurrency;

import cn.xhjava.redis.Jedis_01;
import cn.xhjava.redis.pipline.JedisClusterPipeline;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/5/13
 */
@Slf4j
public class RedisClusterReadThread implements Runnable {
    private JedisClusterPipeline pipeline = JedisClusterPipeline.pipelined(Jedis_01.getJedisCluster());
    private LinkedList<String> keyList;
    private CountDownLatch cdl;

    public RedisClusterReadThread(CountDownLatch cdl) {
        this.cdl = cdl;
    }

    @Override
    public void run() {
        log.info("开始请求Redis Cluster ");

        while (true) {
            this.keyList = RedisMain.dataKey(10000);
            for (String key : keyList) {
                pipeline.get(key);
            }
            long startTime = System.currentTimeMillis();
            List<Object> valuesList = pipeline.syncAndReturnAll();

            long endTime = System.currentTimeMillis();
            log.info("{}  获取数据  {} ,耗费时间 : {} s", Thread.currentThread().getName(), keyList.size(), (endTime - startTime) / 1000.0);
            if (false) {
                cdl.countDown();
            }
        }
    }
}
