package com.clbr.dataxtest.kudu.core.curd.thread;


import com.clbr.dataxtest.kudu.core.curd.InsertTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author XIAHU
 * @create 2019/9/5
 */

public class InsertKuduThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(InsertKuduThread.class);
    private int startId;
    private int endId;
    private CountDownLatch cdl;

    public InsertKuduThread(int startId, int endId, CountDownLatch cdl) {
        this.startId = startId;
        this.endId = endId;
        this.cdl = cdl;
    }


    @Override
    public void run() {
        LOG.info(String.format("线程[%s]启动,正准备插入数据[%s]条", Thread.currentThread().getName(), (endId - startId) + 1));
        try {
            InsertTable.insertByManual(startId, endId);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.info(String.format("线程[%s]即将结束,已经插入数据[%s]条", Thread.currentThread().getName(), (endId - startId) + 1));
        cdl.countDown();
    }
}