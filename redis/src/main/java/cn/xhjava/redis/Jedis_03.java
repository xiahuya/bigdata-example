package cn.xhjava.redis;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author Xiahu
 * @create 2021/4/28
 * <p>
 * 字符串操作
 */
public class Jedis_03 {
    public static void main(String[] args) {
        Jedis jedis = Jedis_01.getJedis();

        // 增加或覆盖数据项
        String set = jedis.set("", "");

        //如果key 在 redis 中存在,则不添加,反之添加;
        jedis.setnx("", "");

        //添加数据并设置有效时间
        String setex = jedis.setex("", 10, "");

        //根据key删除数据
        Long del = jedis.del("");

        //根据key获取数据
        String get = jedis.get("");

        //根据key,在其value后添加内容
        jedis.append("key","append value");

        //一次增加多个键值对
        String mset = jedis.mset("k1", "v1", "k2", "v2");

        // 根据多个key,一次获取多个value
        List<String> mget = jedis.mget("", "", "", "");

        //一次删除多个
        Long del1 = jedis.del("", "", "", "");

        //根据主键获取之前的value,并且设置新value
        String oldValue = jedis.getSet("", "newValue");

        //根据key,获取value 1--5 的字符
        String getrange = jedis.getrange("", 1, 5);
    }
}
