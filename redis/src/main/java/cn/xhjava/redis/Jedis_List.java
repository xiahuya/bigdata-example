package cn.xhjava.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author Xiahu
 * @create 2021/4/28
 * <p>
 * List 操作
 */
public class Jedis_List {
    private Jedis jedis;

    @Before
    public void init() {
        jedis = new Jedis("node2");
    }

    @Test
    public void addList() {
        //添加List
        //返回列表的个数
        Long lpush = jedis.lpush("2", "1", "3", "2");
        System.out.println(lpush);
    }

    @Test
    public void rpush() {
        //从右边添加一个元素
        Long zairian = jedis.rpush("1", "zairian");


    }


    @Test
    public void listRange() {
        List<String> lrange = jedis.lrange("1", 0, 12);
        for (String line : lrange) {
            System.out.println(line);
        }
    }

    @Test
    public void delete() {
        //在list内删除10个value = name
        // 返回被删除元素的个数
        Long lrem = jedis.lrem("1", 10, "name");
        System.out.println(lrem);
    }

    @Test
    public void deleteRange() {
        //删除list内 下标 0 - 3  之外的元素
        String ltrim = jedis.ltrim("1", 0, 4);
        System.out.println(ltrim);
    }

    @Test
    public void lpop() {
        //从list左边出一个元素
        String lpop = jedis.lpop("1");
        System.out.println(lpop);
    }

    @Test
    public void rpop() {
        //从list右边出一个元素
        String lpop = jedis.rpop("1");
        System.out.println(lpop);
    }

    @Test
    public void len() {
        //list长度
        Long llen = jedis.llen("1");
        System.out.println(llen);
    }

    @Test
    public void index() {
        //根据下标获取数据
        String lindex = jedis.lindex("1", 1);
        System.out.println(lindex);
    }

    @Test
    public void sort() {
        //排序
        List<String> lindex = jedis.sort("2");
        for (String line : lindex) {
            System.out.println(line);
        }
    }


    @After
    public void close() {
        jedis.close();
    }

}

