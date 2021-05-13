package cn.xhjava.redis.high_concurrency;

import cn.xhjava.redis.Jedis_01;
import cn.xhjava.redis.pipline.JedisClusterPipeline;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/5/13
 * 多线程从多个Redis 单机实例读取数据
 */
@Slf4j
public class RedisInstanceReadThread implements Runnable {
    private LinkedList<String> data;
    private CountDownLatch cdl;
    private List<Jedis> jedisPools;

    public RedisInstanceReadThread(CountDownLatch cdl) {
        jedisPools = Jedis_01.getJedisPools();
        this.cdl = cdl;
    }

    @Override
    public void run() {
        log.info("开始请求Redis Cluster ");

        while (true) {
            this.data = RedisMain.dataKey(10000);
            long startTime = System.currentTimeMillis();

            Map<Jedis, LinkedList<String>> jedisMapMap = Jedis_01.getDistributeRedisInstacne(jedisPools, data);
            Iterator<Map.Entry<Jedis, LinkedList<String>>> entryIterator = jedisMapMap.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<Jedis, LinkedList<String>> entry = entryIterator.next();
                Pipeline pipelined = entry.getKey().pipelined();
                for (String key : entry.getValue()) {
                    pipelined.get(key);
                }
                pipelined.syncAndReturnAll();
            }


            long endTime = System.currentTimeMillis();
            log.info("{}  读取数据  {} , 耗费时间 : {} s", Thread.currentThread().getName(), data.size(), (endTime - startTime) / 1000.0);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (false) {
                cdl.countDown();
            }
        }
    }
}
