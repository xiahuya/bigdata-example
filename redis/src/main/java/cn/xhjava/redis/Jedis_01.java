package cn.xhjava.redis;

import cn.xhjava.redis.pipline.JedisClusterPipeline;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/4/27
 */
public class Jedis_01 {
    //connect

    public static void main(String[] args) {
        JedisCluster jedisCluster = Jedis_01.getJedisCluster();
        jedisCluster.set("key_1", "xiahu");
        jedisCluster.close();

    }


    public static Jedis getJedis() {
//        return new Jedis("node2");
        return new Jedis("node3", 7000);
    }

    public static Jedis getJedisByPool() {
        //连接池连接
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        //最大连接数
        poolConfig.setMaxTotal(30);
        //最大空闲数
        poolConfig.setMaxIdle(10);
        JedisPool jedisPool = new JedisPool(poolConfig, "node3", 7000);
        Jedis jedis = null;
        try {
            //从连接池获取jedis对象
            jedis = jedisPool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                //这里使用的close不代表关闭连接，指的是归还资源
                jedisPool.close();
            }

        }

        return jedis;
    }

    public static JedisCluster getJedisCluster() {
        Set<HostAndPort> hostAndPortSet = new HashSet<HostAndPort>();
        hostAndPortSet.add(new HostAndPort("node3", 7000));
        hostAndPortSet.add(new HostAndPort("node3", 7004));
        hostAndPortSet.add(new HostAndPort("node3", 7002));

        JedisCluster jedisCluster = new JedisCluster(hostAndPortSet, Jedis_01.getGenericObjectPoolConfig());
        return jedisCluster;
    }

    public static JedisClusterPipeline getJedisClusterPipline() {
        Set<HostAndPort> hostAndPortSet = new HashSet<HostAndPort>();
        hostAndPortSet.add(new HostAndPort("node3", 7000));
        hostAndPortSet.add(new HostAndPort("node3", 7004));
        hostAndPortSet.add(new HostAndPort("node3", 7002));

        return new JedisClusterPipeline(hostAndPortSet, Jedis_01.getGenericObjectPoolConfig());

    }

    private static GenericObjectPoolConfig getGenericObjectPoolConfig() {
        GenericObjectPoolConfig genericObjectPool = new GenericObjectPoolConfig();
       /* genericObjectPool.setMaxIdle(10);
        genericObjectPool.setMaxTotal(100);*/
        genericObjectPool.setMinEvictableIdleTimeMillis(30000);
        genericObjectPool.setSoftMinEvictableIdleTimeMillis(60000);
        return genericObjectPool;
    }

}
