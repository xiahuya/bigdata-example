package cn.xhjava.redis;

import cn.xhjava.redis.pipline.JedisClusterPipeline;
import cn.xhjava.redis.pipline.RedisClusterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
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
    JedisClusterPipeline pipline = null;
    private Pipeline pipelined;


    @Before
    public void init() {
        jedis = new Jedis("node2");
        startTime = System.currentTimeMillis();

        pipelined = jedis.pipelined();
        pipline = JedisClusterPipeline.pipelined(Jedis_01.getJedisCluster());
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
        Map<String, String> map = new HashMap<>(13500000);
        for (int j = 1; j <= 45; j++) {
            String tableNmae = "redis_test_" + j;
            int count = 300000;
            for (int i = 1; i <= count; i++) {
                pipline.set(tableNmae + "_" + i, String.format(FORMAT, i, "zhangsan_" + tableNmae, tableNmae));
            }
        }
        System.out.println("start");

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
        pipline.close();
        //System.out.println("耗费时间：" + (endTime - startTime) / 1000.0);
    }
}
