package cn.xhjava.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.*;

/**
 * @author Xiahu
 * @create 2021/4/27
 * <p>
 * connection
 */
public class Jedis_01 {
    public static void main(String[] args) throws IOException {
        JedisCluster jedisCluster = Jedis_01.getJedisCluster();
        jedisCluster.set("key_1", "xiahu");
        jedisCluster.close();

    }


    public static Jedis getJedis() {
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
        hostAndPortSet.add(new HostAndPort("192.168.0.101", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.102", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.103", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.104", 6458));
        hostAndPortSet.add(new HostAndPort("192.168.0.105", 6458));
        JedisCluster jedisCluster = new JedisCluster(hostAndPortSet, Jedis_01.getGenericObjectPoolConfig());
        return jedisCluster;
    }

    public static List<Jedis> getJedisPools() {
        LinkedList<Jedis> jedisList = new LinkedList<>();
        jedisList.add(new Jedis("192.168.0.105", 7000));
        jedisList.add(new Jedis("192.168.0.105", 7001));
        jedisList.add(new Jedis("192.168.0.105", 7002));
        jedisList.add(new Jedis("192.168.0.105", 7003));
        jedisList.add(new Jedis("192.168.0.105", 7004));
        jedisList.add(new Jedis("192.168.0.105", 7005));
        jedisList.add(new Jedis("192.168.0.105", 7006));
        jedisList.add(new Jedis("192.168.0.105", 7007));
        return jedisList;
    }

    public static void closeJedis(List<Jedis> jedisList) {
        for (Jedis jedis : jedisList) {
            jedis.close();
        }
    }

    public static Map<Jedis, Map<String, String>> distributeRedisInstacne(List<Jedis> jedisList, Map<String, String> data) {
        Map<Jedis, Map<String, String>> result = new HashMap<>();
        Iterator<Map.Entry<String, String>> entryIterator = data.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            Jedis jedis = jedisList.get(Math.abs(entry.getKey().hashCode()) % jedisList.size());
            if (result.containsKey(jedis)) {
                result.get(jedis).put(entry.getKey(), entry.getValue());
            } else {
                Map<String, String> map = new HashMap<>();
                map.put(entry.getKey(), entry.getValue());
                result.put(jedis, map);
            }
        }
        return result;
    }

    public static Map<Jedis, LinkedList<String>> getDistributeRedisInstacne(List<Jedis> jedisList, LinkedList<String> data) {
        Map<Jedis, LinkedList<String>> result = new HashMap<>();
        for (String key : data) {
            Jedis jedis = jedisList.get(Math.abs(key.hashCode()) % jedisList.size());
            if (result.containsKey(jedis)) {
                result.get(jedis).add(key);
            } else {
                LinkedList<String> list = new LinkedList<>();
                list.add(key);
                result.put(jedis, list);
            }
        }
        return result;
    }


    private static GenericObjectPoolConfig getGenericObjectPoolConfig() {
        GenericObjectPoolConfig genericObjectPool = new GenericObjectPoolConfig();
        genericObjectPool.setMaxIdle(10);
        genericObjectPool.setMaxTotal(100);
        genericObjectPool.setMaxWaitMillis(Integer.MAX_VALUE);
        genericObjectPool.setMinEvictableIdleTimeMillis(30000);
        genericObjectPool.setSoftMinEvictableIdleTimeMillis(60000);
        return genericObjectPool;
    }

}
