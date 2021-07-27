package cn.xhjava.server.service.thread;

import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
@Slf4j
public class TestThread implements Callable<String> {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public String call() throws Exception {
        String startTime = sdf.format(new Date());
        log.info("请求进入时间: {}", startTime);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String endTime = sdf.format(new Date());
        log.info("请求出去时间: {}", endTime);
        return String.format("进入时间: %s   当前时间: %s ", startTime, endTime);
    }
}
