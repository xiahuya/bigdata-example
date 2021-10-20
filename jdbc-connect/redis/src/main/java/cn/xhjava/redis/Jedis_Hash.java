package cn.xhjava.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/4/28
 * <p>
 * List 操作
 */
public class Jedis_Hash {
    private Jedis jedis;

    @Before
    public void init() {
        jedis = new Jedis("node2");
    }

    @Test
    public void addHash() {
        //添加一个hash
        HashMap<String, String> map = new HashMap<>();
        map.put("name", "1");
        map.put("age", "20");
        map.put("sex", "男");
        jedis.hmset("1", map);
    }

    @Test
    public void addHash2() {
        //添加K-V对
        jedis.hset("1", "city", "shanghai");
    }

    @Test
    public void getHash() {
        Map<String, String> map = jedis.hgetAll("1");
        Set<String> hkeys = jedis.hkeys("1");
        List<String> hvals = jedis.hvals("1");
        Long hlen = jedis.hlen("1");
        Boolean name = jedis.hexists("1", "name");
        List<String> hmget = jedis.hmget("1", "name", "age");


    }


    @After
    public void close() {
        jedis.close();
    }

}

