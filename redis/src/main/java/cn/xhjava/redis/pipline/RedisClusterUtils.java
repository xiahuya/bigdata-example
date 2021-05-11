package cn.xhjava.redis.pipline;

import cn.xhjava.redis.Jedis_01;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/28
 */
public class RedisClusterUtils {
    private final static Logger log = LoggerFactory.getLogger(RedisClusterUtils.class);





    public static void main(String[] args) throws IOException {
        JedisCluster jedisCluster = Jedis_01.getJedisCluster();
        JedisClusterPipeline pipelined = JedisClusterPipeline.pipelined(jedisCluster);
        for (int i = 1; i <= 100000; i++) {
            pipelined.set("key_" + i, "value_" + i);
        }

        pipelined.close();
        jedisCluster.close();

    }
}
