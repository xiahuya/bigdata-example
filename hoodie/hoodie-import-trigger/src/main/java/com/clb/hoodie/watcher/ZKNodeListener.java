package com.clb.hoodie.watcher;

import com.clb.hoodie.domain.HoodieImportParameter;
import com.clb.hoodie.log.LogFileName;
import com.clb.hoodie.log.LoggerUtil;
import com.clb.hoodie.util.HoodieImportUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2020/7/8
 */
public class ZKNodeListener implements Runnable {
    /*private static Logger log = LoggerFactory.getLogger(ZKNodeListener.class);*/
    private static Logger log = LoggerUtil.logger(LogFileName.SparkProducer);
    private String info = "%s ,Path: %s ,Data: %s";

//    private static final String CONNECT_ADDR = "192.168.0.113:2181,192.168.0.114:2181,192.168.0.115:2181";
    private static final String CONNECT_ADDR = "192.168.0.28:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private static final String ROOT_PATH = "/flink";
    private CountDownLatch countDownLatch;
    private static LinkedList<HoodieImportParameter> list = null;

    private Properties properties;

    public ZKNodeListener(Properties properties, CountDownLatch countDownLatch, LinkedList<HoodieImportParameter> list) {
        this.properties = properties;
        this.countDownLatch = countDownLatch;
        this.list = list;
    }

    public void nodeListenerStart() {
        log.info("监控 zookeeper [ /flink] 节点");
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(CONNECT_ADDR).sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(policy).build();
        curator.start();
        TreeCache treeCache = new TreeCache(curator, ROOT_PATH);
        try {
            treeCache.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        treeCache.getListenable().addListener((curatorFramework, treeCacheEvent) -> {
            switch (treeCacheEvent.getType()) {
                case NODE_ADDED:
                    ChildData data = treeCacheEvent.getData();
                    if (data.getPath().equals(ROOT_PATH)) {
                        break;
                    }
                    synchronized (list.getClass()) {
                        list.addLast(HoodieImportUtil.parseZkData(new String(data.getData())));
                    }
                    log.info(String.format(info, "node-add", data.getPath(), new String(data.getData())));
                    break;
                case NODE_UPDATED:
                    ChildData data1 = treeCacheEvent.getData();
                    if (data1.getPath().equals(ROOT_PATH)) {
                        break;
                    }
                    synchronized (list.getClass()) {
                        list.addLast(HoodieImportUtil.parseZkData(new String(data1.getData())));
                    }
                    log.info(String.format(info, "node-update", data1.getPath(), new String(data1.getData())));
                    break;
                case NODE_REMOVED:
                    ChildData data2 = treeCacheEvent.getData();
                    if (data2.getPath().equals(ROOT_PATH)) {
                        break;
                    }
                    synchronized (list.getClass()) {
                        list.addLast(HoodieImportUtil.parseZkData(new String(data2.getData())));
                    }
                    log.info(String.format(info, "node-remove", data2.getPath(), new String(data2.getData())));
                    break;
                default:
                    break;
            }
        });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        curator.close();
    }

    @Override
    public void run() {
        nodeListenerStart();
    }

}
