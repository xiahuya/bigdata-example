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
        log.debug(params);

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
            String result = EntityUtils.toString(entity, "UTF-8");
            //log.debug(result);
        }

        log.info("请求ElastucSearch 耗费时间: {} s", (endTime - startTime) / 1000.0);
        log.info(params);
        log.info("===============================================================");
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

    private static String param = "{\"size\":0,\"query\":{\"bool\":{\"should\":[{\"range\":{\"executeddate\":{\"gte\":\"%s-01-01T00:00:00Z\",\"lte\":\"%s-01-01T00:00:00Z\",\"time_zone\":\"Asia/Shanghai\"}}},{\"range\":{\"executeddate\":{\"gte\":\"%s-01-01T00:00:00Z\",\"lte\":\"%s-01-01T00:00:00Z\",\"time_zone\":\"Asia/Shanghai\"}}}],\"minimum_should_match\":1,\"must\":[{\"term\":{\"isdeleted\":{\"value\":\"0\"}}}]}},\"aggs\":{\"dept_aggs\":{\"terms\":{\"field\":\"deptid\",\"size\":%s,\"order\":{\"_count\":\"desc\"}},\"aggs\":{\"cy\":{\"filter\":{\"range\":{\"executeddate\":{\"gte\":\"%s-01-01T00:00:00Z\",\"lte\":\"%s-01-01T00:00:00Z\",\"time_zone\":\"Asia/Shanghai\"}}},\"aggs\":{\"summoney\":{\"sum\":{\"field\":\"totalmoney\"}}}},\"ly\":{\"filter\":{\"range\":{\"executeddate\":{\"gte\":\"%s-01-01T00:00:00Z\",\"lte\":\"%s-01-01T00:00:00Z\",\"time_zone\":\"Asia/Shanghai\"}}},\"aggs\":{\"summoney\":{\"sum\":{\"field\":\"totalmoney\"}}}},\"joy\":{\"bucket_script\":{\"buckets_path\":{\"cy1\":\"cy.summoney\",\"ly1\":\"ly.summoney\"},\"script\":\"(params.cy1-params.ly1)/params.ly1\"}}}}}}";

    public static String randomGetParams() {
        Integer year = Integer.valueOf(partitionList.get(random(random, 0, partitionList.size())));
//        int flag = random(HttpHelper.random, 1, 4);
        int flag = 1;
        return String.format(param, year, year + flag, year - flag, year, random(HttpHelper.random, 10, 1000), year, year + flag, year - flag, year);
    }

    private static int random(Random random, int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }

    private static String SQL = "{\"query\":\"%s\"}";

    public static String requestBySql(String sql) {
        return String.format(SQL, sql);
    }
}
