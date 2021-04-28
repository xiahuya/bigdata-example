package cn.xhjava.redis.pipline;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021/4/28
 * <p>
 * 使用Pipline 写入redis
 */
public class JedisPiplineThread implements Runnable {

    private Jedis jedis;
    private Map<String, String> data;
    private CountDownLatch cdl;
    private Pipeline pipelined;

    public JedisPiplineThread(Jedis jedis, Map<String, String> data, CountDownLatch cdl) {
        this.jedis = jedis;
        this.data = data;
        this.cdl = cdl;

    }


    @Override
    public void run() {
        try {
            Iterator<Map.Entry<String, String>> iterator = data.entrySet().iterator();
            pipelined = jedis.pipelined();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                pipelined.set(entry.getKey(), entry.getValue());
            }
            pipelined.sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pipelined.close();
            jedis.close();
            cdl.countDown();
        }
    }
}
