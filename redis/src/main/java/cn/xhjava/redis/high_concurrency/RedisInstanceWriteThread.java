package cn.xhjava.redis.high_concurrency;

import cn.xhjava.redis.Jedis_01;
import cn.xhjava.redis.pipline.JedisClusterPipeline;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/5/13
 * <p>
 * 多线程写入多个Redis 单机实例
 */
@Slf4j
public class RedisInstanceWriteThread implements Runnable {
    private Map<String, String> data;
    private CountDownLatch cdl;
    private List<Jedis> jedisPools;

    public RedisInstanceWriteThread(CountDownLatch cdl) {
        this.cdl = cdl;
        jedisPools = Jedis_01.getJedisPools();
    }

    @Override
    public void run() {
        log.info("开始请求Redis Cluster ");

        while (true) {
            this.data = RedisMain.dataKeyValue(10000);
            long startTime = System.currentTimeMillis();

            Map<Jedis, Map<String, String>> jedisMapMap = Jedis_01.distributeRedisInstacne(jedisPools, data);
            Iterator<Map.Entry<Jedis, Map<String, String>>> entryIterator = jedisMapMap.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<Jedis, Map<String, String>> entry = entryIterator.next();
                Pipeline pipelined = entry.getKey().pipelined();
                Iterator<Map.Entry<String, String>> iterator = entry.getValue().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> next = iterator.next();
                    pipelined.set(next.getKey(), next.getValue());
                }
                pipelined.sync();
            }


            long endTime = System.currentTimeMillis();
            log.info("{}  写入数据  {} , 耗费时间 : {} s", Thread.currentThread().getName(), data.size(), (endTime - startTime) / 1000.0);
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
