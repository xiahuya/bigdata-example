package com.clb.hoodie;

import com.clb.hoodie.domain.HoodieImportParameter;
import com.clb.hoodie.log.LogFileName;
import com.clb.hoodie.log.LoggerUtil;
import com.clb.hoodie.socket.SparkShellServer;
import com.clb.hoodie.watcher.ZKNodeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2020/7/9
 * java -jar /home/xiahu/hoodieTrigger-jar-with-dependencies.jar -class com.clb.hoodie.Main
 */
public class Main {
    //    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static Logger log = LoggerUtil.logger(LogFileName.SparkProducer);

    private static CountDownLatch countDownLatch;
    private static ZKNodeListener zkNodeListener;
    private static LinkedList<HoodieImportParameter> list = null;

    public static void main(String[] args) {
        list = new LinkedList<>();
        countDownLatch = new CountDownLatch(1);
        zkNodeListener = new ZKNodeListener(new Properties(), countDownLatch, list);
        //启动一个线程去监控zkNode
        new Thread(zkNodeListener).start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //监控list size
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    log.info("任务剩余个数：" + list.size());
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        Properties properties = new Properties();
        properties.put("hoodie.import.by.shell.config.path", "/home/xiahu/hoodieImport.properties");
        SparkShellServer server = new SparkShellServer(list, properties);
//        try {
//            server.executSparkShellCommand();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


    }
}
