package cn.xhjava.http.thread;

import cn.xhjava.http.util.HttpHelper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
@Slf4j
public class HttpRequestThread implements Runnable {

    private String api;
    private String json;
    private int lookNum;
    private CountDownLatch count;

    public HttpRequestThread(String api, String json, int lookNum, CountDownLatch count) {
        this.api = api;
        this.json = json;
        this.count = count;
        this.lookNum = lookNum;
    }


    @Override
    public void run() {
        log.info("{} 线程进来了", Thread.currentThread().getName());
        for (int i = 0; i < lookNum; i++) {
            try {
                HttpHelper.doPost(api, json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        count.countDown();
    }
}
