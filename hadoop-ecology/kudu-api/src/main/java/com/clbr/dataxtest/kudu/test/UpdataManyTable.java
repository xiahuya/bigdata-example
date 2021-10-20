package com.clbr.dataxtest.kudu.test;


import com.clbr.dataxtest.kudu.core.curd.thread.UpdataManyTableThread;
import com.clbr.dataxtest.kudu.util.Prop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author XIAHU
 * @create 2019/9/9
 */

public class UpdataManyTable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdataManyTable.class);
    private Prop prop = new Prop();

    public void updateManyTable(Integer updataDataCount, Integer tableCount) {
        long start = System.currentTimeMillis();
        String tableName = prop.getKuduTable();
        if (!tableName.endsWith("_")) {
            tableName = "test_%s";
        }

        //线程池
        ExecutorService pool = Executors.newCachedThreadPool();
        //计数器,
        CountDownLatch cdl = new CountDownLatch(tableCount);
        for (int i = 1; i <= tableCount; i++) {
            tableName = String.format(tableName, i);
            //启动线程
            pool.execute(new UpdataManyTableThread(tableName, updataDataCount, cdl));
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
            LOG.error("请输入:每张表要更新的数据量    想要更新的表数量,(启动时将配置文件放置/home/KLBR路径下)");
        } else {
            Integer updataDataCount = Integer.valueOf(args[0]);
            Integer tableCount = Integer.valueOf(args[1]);
            UpdataManyTable table = new UpdataManyTable();
            table.updateManyTable(updataDataCount,tableCount);
        }
    }

}