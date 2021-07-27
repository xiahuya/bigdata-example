package cn.xhjava.server.service.impl;

import cn.xhjava.server.service.ElasticSearchJdbcRequestService;
import cn.xhjava.server.service.thread.TestThread;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
@Service
public class ElasticSearchJdbcRequestServiceImpl implements ElasticSearchJdbcRequestService {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private ExecutorService executorService = Executors.newFixedThreadPool(100);

    @Override
    public Future<String> selectDataBySql(String sql) {
        return executorService.submit(new TestThread());
    }
}
