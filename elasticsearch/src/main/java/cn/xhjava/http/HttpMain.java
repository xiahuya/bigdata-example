package cn.xhjava.http;

import cn.xhjava.http.threads.HttpRequestByThread;
import cn.xhjava.http.util.HttpHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Xiahu
 * @create 2021-07-12
 */
@Slf4j
public class HttpMain {

    /**
     * java -cp   http-request-1.0-SNAPSHOT-jar-with-dependencies.jar cn.xhjava.HttpMain  10 1 http://192.168.0.114:9213/cdr/_search
     *
     * @param args
     */
    public static void main(String[] args) {
        new HttpMain().start(100, 1, "http://192.168.0.114:9213/ab_ip_feelist/_search");
//        new HttpMain().start(Integer.valueOf(args[0]), Integer.valueOf(args[1]), args[2]);
    }


    public void start(int threadCount, int loopNum, String esApi) {
        long startTime = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int j = 1; j <= threadCount; j++) {
            executorService.submit(new HttpRequestByThread(esApi, loopNum, cdl));
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
