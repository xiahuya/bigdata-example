package cn.xhjava.redis.pipline;

import cn.xhjava.redis.Jedis_01;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021/4/28
 */
public class RedisClusterUtils {
    private final static Logger log = LoggerFactory.getLogger(RedisClusterUtils.class);


    /**
     * 根据key,找到对应的solts与redis 节点
     * 使用对应节点Jedis.pipline 写数据
     *
     * @param jedisClusterPipeline
     * @param data
     */
    public static void addByPipline(JedisClusterPipeline jedisClusterPipeline, Map<String, String> data) {
        long startTime = System.currentTimeMillis();

        JedisSlotAdvancedConnectionHandler handler = jedisClusterPipeline.getConnectionHandler();
        //刷新集群solts信息
        jedisClusterPipeline.refreshCluster();
        //每个jedisPool 对应内部的数据
        Map<JedisPool, Map<String, String>> poolMap = new HashMap<>();
        //遍历key,获取每个key对应的slots
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            int slot = JedisClusterCRC16.getSlot(key);
            JedisPool jedisPool = handler.getJedisPoolFromSlot(slot);
            if (poolMap.containsKey(jedisPool)) {
                Map<String, String> map = poolMap.get(jedisPool);
                map.put(entry.getKey(), entry.getValue());
            } else {
                Map<String, String> map = new HashMap<>();
                map.put(entry.getKey(), entry.getValue());
                poolMap.put(jedisPool, map);
            }
        }

        //多线程写Redis,多线程读写会 Read TimeOut
        /*CountDownLatch cdl = new CountDownLatch(poolMap.size());
        ExecutorService threadPool = Executors.newFixedThreadPool(poolMap.size());
        for (Map.Entry<JedisPool, Map<String, String>> entry : poolMap.entrySet()) {
            threadPool.execute(new JedisPiplineThread(entry.getKey().getResource(), entry.getValue(), cdl));
        }

        try {
            cdl.await();
            long endTime = System.currentTimeMillis();
            log.info("批量写入数据: {} , 耗费时间: {} s", data.size(), (endTime - startTime) / 1000.0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            handler.close();
            jedisClusterPipeline.close();
            threadPool.shutdown();
        }*/


        for (Map.Entry<JedisPool, Map<String, String>> entry : poolMap.entrySet()) {
            Jedis jedis = entry.getKey().getResource();
            Map<String, String> map = entry.getValue();
            Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
            Pipeline pipelined = jedis.pipelined();
            while (iterator.hasNext()) {
                Map.Entry<String, String> map2 = iterator.next();
                pipelined.set(map2.getKey(), map2.getValue());
            }
            try {
                pipelined.sync();
            } catch (Exception e) {
                e.printStackTrace();
            }
            pipelined.close();
            jedis.close();
        }
        long endTime = System.currentTimeMillis();
        log.info("批量写入数据: {} , 耗费时间: {} s", data.size(), (endTime - startTime) / 1000.0);
    }


    public static Map<String, String> getByPipeline(JedisClusterPipeline jedisClusterPipeline, List<String> keyList) {
        JedisSlotAdvancedConnectionHandler handler = jedisClusterPipeline.getConnectionHandler();
        //刷新集群solts信息
        jedisClusterPipeline.refreshCluster();
        Map<JedisPool, LinkedList<String>> poolMap = new HashMap<>();
        for (String key : keyList) {
            int slot = JedisClusterCRC16.getSlot(key);
            JedisPool jedisPool = handler.getJedisPoolFromSlot(slot);
            if (poolMap.containsKey(jedisPool)) {
                LinkedList<String> keys = poolMap.get(jedisPool);
                keys.addLast(key);
            } else {
                LinkedList<String> list = new LinkedList<>();
                list.add(key);
                poolMap.put(jedisPool, list);
            }
        }

        Map<String, String> result = new HashMap<>();

        for (Map.Entry<JedisPool, LinkedList<String>> entry : poolMap.entrySet()) {
            Jedis jedis = entry.getKey().getResource();
            Pipeline pipelined = jedis.pipelined();
            LinkedList<String> values = entry.getValue();
            for (String key : values) {
                pipelined.get(key);
            }

            List<Object> objectList = pipelined.syncAndReturnAll();
            for (Object value : objectList) {
                if (null != value) {
                    String line = (String) value;
                    result.put(values.removeFirst(), line);
                }
            }
        }
        return result;
    }



    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        ArrayList<String> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            map.put("key_" + i, "value_" + i);
            list.add("key_" + i);
        }

        JedisClusterPipeline pipline = Jedis_01.getJedisClusterPipline();
        RedisClusterUtils.addByPipline(pipline, map);
        Map<String, String> stringStringMap = RedisClusterUtils.getByPipeline(pipline, list);
        for (Map.Entry<String, String> entry : stringStringMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
