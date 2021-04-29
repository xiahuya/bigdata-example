package cn.xhjava.redis;

import redis.clients.jedis.Jedis;

/**
 * @author Xiahu
 * @create 2021/4/28
 *
 * 整数和浮点型操作
 */
public class Jedis_04 {
    public static void main(String[] args) {
        Jedis jedis = Jedis_01.getJedis();

        //将key对应的value 自增1
        Long incr = jedis.incr("");

        //将key对应的value 自增10
        Long aLong = jedis.incrBy("", 10);

        //将key对应的value 自减1
        Long decr = jedis.decr("");

        //将key对应的value 自减10
        Long decrBy = jedis.decrBy("", 10);
    }
}
