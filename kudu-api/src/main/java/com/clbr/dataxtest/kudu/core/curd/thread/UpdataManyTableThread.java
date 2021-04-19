package com.clbr.dataxtest.kudu.core.curd.thread;


import com.clbr.dataxtest.kudu.core.curd.UpdataManyTable;
import com.clbr.dataxtest.kudu.core.curd.UpdateTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author XIAHU
 * @create 2019/9/9
 */

public class UpdataManyTableThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdataManyTableThread.class);
    private String updataTableName;
    private Integer updataDataCount;
    private CountDownLatch cdl;

    public UpdataManyTableThread(String updataTableName, Integer updataDataCount, CountDownLatch cdl) {
        this.updataDataCount = updataDataCount;
        this.updataTableName = updataTableName;
        this.cdl = cdl;
    }


    @Override
    public void run() {
        LOG.info(String.format("线程[%s]启动,当前准备Updata表是:[%s],需要Updata表数据量为:[%s]", Thread.currentThread().getName(), updataTableName, updataDataCount));
        try {
            UpdataManyTable.updateTable(updataTableName, updataDataCount);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.info(String.format("线程[%s]即将结束,表[%s]已经完成Updata,Updata数据量为:[%s]", Thread.currentThread().getName(), updataTableName, updataDataCount));
        cdl.countDown();
    }
}