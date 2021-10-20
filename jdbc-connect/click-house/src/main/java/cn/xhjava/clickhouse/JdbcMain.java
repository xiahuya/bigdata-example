package cn.xhjava.clickhouse;

import cn.xhjava.clickhouse.threads.ClickHouseJdbcRequestThread;
import cn.xhjava.connect.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021-08-01
 */
@Slf4j
public class JdbcMain {
    private static TreeSet<Double> treeSet = new TreeSet<>();

    public static void main(String[] args) {
        new JdbcMain().start(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]));
    }


    public void start(int threadCount, int loopNum, int sqlVersion) {
        long startTime = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        ConnectionPool connPool = new ConnectionPool();
        for (int j = 1; j <= threadCount; j++) {
            executorService.submit(new ClickHouseJdbcRequestThread(loopNum, cdl, connPool, treeSet, sqlVersion));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cdl.await();
                    executorService.shutdown();
                    long endTime = System.currentTimeMillis();
                    log.info("总共执行次数: {} ,耗费时间: {} s", (loopNum * threadCount), (endTime - startTime) / 1000.0);
                    int size = treeSet.size();
                    Iterator<Double> iterator = treeSet.iterator();
                    int i = 0;
                    double max = 0D;
                    double min = 0D;
                    double request95 = 0D;
                    while (iterator.hasNext()) {
                        double current = iterator.next();
                        if (i == 0) {
                            min = current;
                        }

                        if (i == size - 1) {
                            max = current;
                        }
                        if (i == (int) Math.ceil(size * 0.95)) {
                            request95 = current;
                        }
                        i++;
                    }
                    log.info("最大时间: {}, 最小时间: {}, 95%请求在 {} 内响应", max, min, request95);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {

                }
            }
        }).start();
    }

}
