package cn.xhjava.redis;

import cn.xhjava.redis.high_concurrency.RedisMain;
import cn.xhjava.redis.pipline.JedisClusterPipeline;
import cn.xhjava.redis.pipline.RedisClusterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2021/5/11
 */
public class RedisTest {
    private Jedis jedis;
    private static String FORMAT = "id:%s,name:%s,age:%s";
    private long startTime;
    JedisClusterPipeline piplineCluster = null;
    private Pipeline pipelined;


    @Before
    public void init() {
        jedis = new Jedis("node2");
        startTime = System.currentTimeMillis();

        pipelined = jedis.pipelined();
        piplineCluster = JedisClusterPipeline.pipelined(Jedis_01.getJedisCluster());
    }


    @Test
    public void insertDataToRedis() {
        for (int j = 1; j <= 45; j++) {
            String tableNmae = "redis_test_" + j;
            int count = 300000;
            for (int i = 1; i <= count; i++) {
                pipelined.set(tableNmae + "_" + i, String.format(FORMAT, i, "zhangsan_" + tableNmae, tableNmae));
            }
            pipelined.sync();
        }
    }

    @Test
    public void insertDataToRedisCluster() {
        for (int j = 1; j <= 45; j++) {
            String tableNmae = "redis_test_" + j;
            int count = 300000;
            for (int i = 1; i <= count; i++) {
                piplineCluster.set(tableNmae + "_" + i, String.format(FORMAT, i, "zhangsan_" + tableNmae, tableNmae));
            }
            piplineCluster.sync();
        }
        System.out.println("start");
    }


    @Test
    public void insertMultiRedisInstance() {
        List<Jedis> jedisPools = Jedis_01.getJedisPools();
        Map<String, String> dataKeyValue = RedisMain.dataKeyValue(1000000);
        Map<Jedis, Map<String, String>> jedisMapMap = Jedis_01.distributeRedisInstacne(jedisPools, dataKeyValue);

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
        Jedis_01.closeJedis(jedisPools);
    }

    @Test
    public void getAllKeyValue() {
        String tableNmae = "redis_test_5_1";
        pipelined.get("redis_test_5_1");
        pipelined.get("redis_test_5_2");
        pipelined.get("redis_test_5_3000000000");
        pipelined.get("redis_test_5_4");
        pipelined.get("redis_test_5_5");
        List<Object> objects = pipelined.syncAndReturnAll();
        for (Object obj : objects) {
            if (null != obj) {
                System.out.println(obj);
            }
        }
    }

    @After
    public void close() {
        jedis.close();
        long endTime = System.currentTimeMillis();
        piplineCluster.close();
        System.out.println("耗费时间：" + (endTime - startTime) / 1000.0);
    }
}
