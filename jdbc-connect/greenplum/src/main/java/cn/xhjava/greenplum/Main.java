package cn.xhjava.greenplum;

import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021-07-07
 */
@Slf4j
public class Main {
    private static String[] fieldNames = {"id", "fk_id", "qfxh", "nioroa", "gwvz", "joqtf", "isdeleted", "lastupdatedttm", "rowkey"};

    public static void main(String[] args) {
        Main main = new Main();
        main.insert(Integer.valueOf(args[0]), Integer.valueOf(args[1]));
//        main.upsert(Integer.valueOf(args[0]), Integer.valueOf(args[1]), 1);
    }


    public void insert(int threadCount, int dataCount) {
        long startTime = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        String table = "xh_test_%s";
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int j = 1; j <= threadCount; j++) {
            executorService.submit(new GreenPlumInsert(
                    String.format(table, j),
                    fieldNames,
                    dataCount,
                    cdl
            ));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cdl.await();
                    executorService.shutdown();
                    long endTime = System.currentTimeMillis();
                    log.info("总共写入数据: {} ,耗费时间: {} s", (dataCount * threadCount), (endTime - startTime) / 1000.0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        GreenPlumHelper.getConn().close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }


    public void upsert(int threadCount, int dataCount, int loopCount) {
        long startTime = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        String table = "xh_test_%s";
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int j = 1; j <= threadCount; j++) {
            executorService.submit(new GreenPlumUpsert(
                    String.format(table, j),
                    dataCount,
                    loopCount,
                    cdl
            ));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cdl.await();
                    executorService.shutdown();
                    long endTime = System.currentTimeMillis();
                    log.info("总共写入数据: {} ,耗费时间: {} s", (dataCount * threadCount * loopCount), (endTime - startTime) / 1000.0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        GreenPlumHelper.getConn().close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}
