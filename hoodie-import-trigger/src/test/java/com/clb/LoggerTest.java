package com.clb;

import com.clb.hoodie.log.LogFileName;
import com.clb.hoodie.log.LoggerUtil;
import org.slf4j.Logger;

/**
 * @author Xiahu
 * @create 2020/7/10
 */
public class LoggerTest {
    public static void main(String[] args) {
        Logger FIRST_LOG = LoggerUtil.logger(LogFileName.SparkProducer);
        Logger SECOND_LOG = LoggerUtil.logger(LogFileName.SparkConsumer);
        FIRST_LOG.info("aaaaa");
        SECOND_LOG.info("bbbbb");
    }

}
