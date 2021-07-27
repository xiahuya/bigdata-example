package cn.xhjava.http.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


/**
 * @author Xiahu
 * @create 2021-07-12
 */
@Slf4j
public class HttpHelper {
    public static CloseableHttpClient httpClient;
    private static Random random = new Random();
    private static List<String> partitionList = Arrays.asList(
            "2012",
            "2013",
            "2014",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019");

    static {
        httpClient = HttpClients.createDefault();
    }

    public static String doPost(String api, String params) throws IOException {
        String result = null;
        long endTime = -1l;
        HttpPost post = new HttpPost(api);
        // 设置提交参数为application/json
        post.addHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity(params, "UTF-8"));
        long startTime = System.currentTimeMillis();
        CloseableHttpResponse response = httpClient.execute(post);
        if (response.getStatusLine().getStatusCode() == 200) {
            endTime = System.currentTimeMillis();
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity, "UTF-8");
            //log.debug(result);
        }

        /*log.info("请求地址: {} , 请求数据: {}-{} , 耗费时间: {} s", api, Thread.currentThread().getName(), params, (endTime - startTime) / 1000.0);
        log.info("返回结果: {}", result);
        log.info("===============================================================");*/
        return null;
    }


    public static String doGet(String api, String params) throws IOException {
        long startTime = System.currentTimeMillis();
        long endTime = -1l;

        HttpGet get = new HttpGet(api);
        // 设置提交参数为application/json
        get.addHeader("Content-Type", "application/json");
        CloseableHttpResponse response = httpClient.execute(get);
        if (response.getStatusLine().getStatusCode() == 200) {
            endTime = System.currentTimeMillis();
        }

        log.info("请求 ElastucSearch 耗费时间: {} s", (endTime - startTime) / 1000.0);
        return null;
    }

}
