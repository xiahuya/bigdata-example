package com.clb;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2020/8/7
 */
public class ZookeeperConnectServer implements Watcher {


    @Override
    public void process(WatchedEvent event) {

    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper("192.168.0.28", 3000, new ZookeeperConnectServer());

        zooKeeper.create("/flink/aaa","hello ".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}
