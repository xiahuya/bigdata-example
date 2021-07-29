package cn.xhjava.server.service.impl;

import cn.xhjava.server.service.ElasticSearchJdbcRequestService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
@Service
@Slf4j
public class ElasticSearchJdbcRequestServiceImpl implements ElasticSearchJdbcRequestService {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    /*@Override
    @Async("asyncServiceExecutor")
    public Future<String> selectDataBySql(String sql) {
//        return executorService.submit(new TestThread());
        String startTime = sdf.format(new Date());
        log.info("请求进入时间: {}", startTime);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String endTime = sdf.format(new Date());
        log.info("请求出去时间: {}", endTime);
        String result = String.format("进入时间: %s   当前时间: %s ", startTime, endTime);
        return new AsyncResult<>(result);
    }*/


    @Override
    @Async("asyncServiceExecutor")
    public String selectDataBySql(String sql) {
//        return executorService.submit(new TestThread());
        String startTime = sdf.format(new Date());
        log.info("请求进入时间: {}", startTime);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String endTime = sdf.format(new Date());
        log.info("请求出去时间: {}", endTime);
        String result = String.format("进入时间: %s   当前时间: %s ", startTime, endTime);
        return result;
    }
}
