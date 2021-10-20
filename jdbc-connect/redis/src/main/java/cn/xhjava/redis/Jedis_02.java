package cn.xhjava.redis;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/4/27
 * <p>
 * key operator
 */
public class Jedis_02 {

    public static void main(String[] args) {
        Jedis jedis = Jedis_01.getJedis();

        //清空数据
        String flushDB = jedis.flushDB();

        //判断key是否存在
        Boolean exists = jedis.exists("");


        //通过正则获取所有key
        Set<String> keys = jedis.keys("*");


        //设置key的过期时间
        Long expire = jedis.expire("", 10);

        //根据key获取该数据的剩余生存时间
        Long ttl = jedis.ttl("");

        //移除生存时间的限制
        Long persist = jedis.persist("");

        //查看key 对应value的数据类型
        String type = jedis.type("");

    }
}
