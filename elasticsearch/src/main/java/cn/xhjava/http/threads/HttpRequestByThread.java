package cn.xhjava.http.threads;

import cn.xhjava.http.util.BuildSql;
import cn.xhjava.http.util.HttpHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-07-12
 */
public class HttpRequestByThread implements Runnable {

    private CountDownLatch cdl;
    private int loopNumber;
    private String apiUrl;
    private BuildSql buildSql;


    public HttpRequestByThread(String apiUrl, int loopNumber, CountDownLatch cdl) {
        this.cdl = cdl;
        this.loopNumber = loopNumber;
        this.apiUrl = apiUrl;
        buildSql = new BuildSql();
    }

    @Override
    public void run() {
        for (int i = 0; i < loopNumber; i++) {
            try {
                HttpHelper.doPost(randomGetApi(), HttpHelper.requestBySql(buildSql.buildRandomSql(2010, 2021)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        cdl.countDown();
    }

    private String randomGetApi() {
        ArrayList<String> list = new ArrayList<>();
//        list.add("http://100.72.12.107:9201/_sql?format=json");
//        list.add("http://100.72.12.107:9202/_sql?format=json");
//        list.add("http://100.72.12.107:9203/_sql?format=json");
//        list.add("http://100.72.12.107:9204/_sql?format=json");
        list.add("http://192.168.0.114:9213/_sql?format=json");

        return list.get(random(0, list.size()));
    }

    private int random(int min, int max) {
        Random random = new Random();
        return random.nextInt(max) % (max - min + 1) + min;
    }
}
