package com.clbr.dataxtest.kudu.test;


import com.clbr.dataxtest.kudu.core.curd.CreateTable;
import com.clbr.dataxtest.kudu.core.curd.DeleteTable;
import com.clbr.dataxtest.kudu.core.curd.thread.InsertKuduThread;
import com.clbr.dataxtest.kudu.core.curd.thread.UpdateKuduThread;
import org.apache.kudu.client.KuduException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author XIAHU
 * @create 2019/9/5
 */

public class KuduTest {
    private static final Logger LOG = LoggerFactory.getLogger(KuduTest.class);

    @Test
    public void createTable() throws KuduException {
        CreateTable.createTable();
    }


    //插入数据
    public void insertKudu(int dataCount, int threadCount) {
        long start = System.currentTimeMillis();
        int count = dataCount / threadCount;
        int startId;
        int endId;

        //线程池
        ExecutorService pool = Executors.newCachedThreadPool();
        //计数器,
        CountDownLatch cdl = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            startId = (count * i);
            if (i == threadCount - 1) {
                endId = dataCount;
            } else {
                endId = startId + count;
            }
            startId = startId + 1;
            //启动线程
            pool.execute(new InsertKuduThread(startId, endId, cdl));
        }
        try {
            //等待，等待全部线程执行完毕才执行
            cdl.await();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
        long end = System.currentTimeMillis();
        LOG.info(String.format("共耗费时间[%s]s", (end - start) / 1000.0));
        pool.shutdown();

    }


    //更新数据
    public void updateKudu(int dataCount, int threadCount) {
        long start = System.currentTimeMillis();
        int count = dataCount / threadCount;
        int startId;
        int endId;

        //线程池
        ExecutorService pool = Executors.newCachedThreadPool();
        //计数器,
        CountDownLatch cdl = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            startId = (count * i);
            if (i == threadCount - 1) {
                endId = dataCount;
            } else {
                endId = startId + count;
            }
            startId = startId + 1;
            //启动线程
            pool.execute(new UpdateKuduThread(startId, endId, cdl));
        }
        try {
            //等待，等待全部线程执行完毕才执行
            cdl.await();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
        long end = System.currentTimeMillis();
        LOG.info(String.format("共耗费时间[%s]s", (end - start) / 1000.0));
        pool.shutdown();
    }

        public static void main(String[] args) {
        if (args.length != 2) {
            LOG.error("请输入:数据量    启动线程数,(启动时将配置文件放置/home/KLBR路径下)");
        } else {
            Integer rowCount = Integer.valueOf(args[0]);
            Integer threadCount = Integer.valueOf(args[1]);
            KuduTest kudu = new KuduTest();
            kudu.updateKudu(rowCount, threadCount);
        }
    }



    @Test
    public void deleteTable() throws KuduException {
        DeleteTable.deleteTable();
    }


}