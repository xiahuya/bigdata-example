package cn.xhjava.redis.high_concurrency;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021/5/13
 * <p>
 * redis 集群高并发请求测试
 */
@Slf4j
public class RedisMain {
    public static int threadCount = 200;
    private static Random random = new Random();


    public static void main(String[] args) {

        List<Map<String, String>> dataList = new ArrayList<>();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        //LinkedList<String> data = dataKey(1000000);
        for (int i = 0; i < threadCount; i++) {
            //service.submit(new RedisClusterReadThread(cdl));
            service.submit(new RedisClusterWriteThread(cdl));
//            service.submit(new RedisInstanceReadThread(null, cdl));
//            service.submit(new RedisInstanceReadThread(null, cdl));
        }

        new Thread(() -> {
            try {
                cdl.await();
                service.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static String tableModel = "xh.testTable_%s_%s";

    public static LinkedList<String> dataKey(int count) {
        LinkedList<String> result = new LinkedList<>();
        for (int key = 1; key < count; key++) {
            int table = random(RedisMain.random, 1, 45);
            //key = random(RedisMain.random, 1, 300000);
            String format = String.format(tableModel, table, key);
            result.addLast(format);
        }
        return result;
    }

    public static Map<String, String> dataKeyValue(int count) {
        Map<String, String> result = new HashMap<>();
        int table = random(RedisMain.random, 1, 45);
        for (int i = 1; i <= count; i++) {
            //key = random(RedisMain.random, 1, 300000);
            String format = String.format(tableModel, table, i);
            result.put(format, format);
        }
        return result;
    }


    public static int random(Random random, int min, int max) {
        Random randoms = new Random();
        return randoms.nextInt(max) % (max - min + 1) + min;
    }
}
