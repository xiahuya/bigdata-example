package cn.xhjava.http.main;

import cn.xhjava.http.thread.HttpRequestThread;
import cn.xhjava.http.util.HttpHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
@Slf4j
public class HttpMain {
    public static void main(String[] args) {
        new HttpMain().start(100, 1, "http://localhost:8080/hello/world", "xiahu");
    }

    public void start(int threadCount, int loopNum, String api, String json) {
        long startTime = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int j = 1; j <= threadCount; j++) {
            executorService.submit(new HttpRequestThread(api, json, loopNum, cdl));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cdl.await();
                    executorService.shutdown();
                    long endTime = System.currentTimeMillis();
                    log.info("总共执行次数: {} ,耗费时间: {} s", (loopNum * threadCount), (endTime - startTime) / 1000.0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        HttpHelper.httpClient.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }
}
