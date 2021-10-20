package cn.xhjava.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/4/29
 * 有序集合
 */
public class Jedis_ZSort {
    private Jedis jedis;

    @Before
    public void init() {
        jedis = new Jedis("node2");
    }

    @Test
    public void addZSort() {
        //添加
        HashMap<String, Double> map = new HashMap<>();
        map.put("name", 1.0);
        map.put("age", 2.0);
        map.put("sex", 3.0);
        jedis.zadd("1", map);
    }

    @Test
    public void other() {
        Set<String> zrange = jedis.zrange("1", 0, 1);

    }


    @After
    public void close() {
        jedis.close();
    }
}
