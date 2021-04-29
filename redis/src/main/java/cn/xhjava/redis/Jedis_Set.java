package cn.xhjava.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/4/28
 * <p>
 * List 操作
 */
public class Jedis_Set {
    private Jedis jedis;

    @Before
    public void init() {
        jedis = new Jedis("node2");
    }

    @Test
    public void addSet() {
        jedis.sadd("1", "zhangsan", "lisi", "wangwu", "zhaoliu");
        jedis.sadd("2", "zhangsan1", "lisi", "wangwu2", "zhaoliu");
    }


    @Test
    public void getSet() {
        //获取set内所有元素
        Set<String> smembers = jedis.smembers("1");
        for (String line : smembers) {
            System.out.println(line);
        }
    }

    @Test
    public void delete() {
        Long zhaoliu = jedis.srem("1", "zhaoliu");
    }

    @Test
    public void pop() {
        //随机返回一个元素
        String spop = jedis.spop("1");
        System.out.println(spop);
    }

    @Test
    public void len() {
        //返回set 元素个数
        Long len = jedis.scard("1");
        System.out.println(len);
    }

    @Test
    public void set() {
        System.out.println("=========交集==========");
        Set<String> sinter = jedis.sinter("1", "2");
        for (String line : sinter) {
            System.out.println(line);
        }

        System.out.println("=========并集==========");
        Set<String> sunion = jedis.sunion("1", "2");
        for (String line : sunion) {
            System.out.println(line);
        }

        System.out.println("=========差集==========");
        Set<String> sdiff = jedis.sdiff("1", "2");
        for (String line : sdiff) {
            System.out.println(line);
        }
    }


    @After
    public void close() {
        jedis.close();
    }

}

