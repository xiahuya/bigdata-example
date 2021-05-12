package cn.xhjava.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021/4/28
 */
public class Jedis_pipline {
    public static void main(String[] args) {
        Jedis_pipline.addDataByPipline();

    }


    //通过Pipline添加数据
    public static void addDataByPipline() {
        Jedis jedis = Jedis_01.getJedis();
        Pipeline pipelined = jedis.pipelined();

        for (int i = 1; i <= 100; i++) {
            pipelined.set("key_" + i, "value_" + i);
        }
        pipelined.sync();


        //释放资源
        try {
            pipelined.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        jedis.close();

    }
}
