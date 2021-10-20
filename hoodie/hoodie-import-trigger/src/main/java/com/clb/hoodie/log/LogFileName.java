package com.clb.hoodie.log;

import org.apache.commons.lang.StringUtils;

/**
 * @author Xiahu
 * @create 2020/7/10
 */
public enum LogFileName {

    // 与logback.xml的logger name 相同
    SparkProducer("ZKNodeListener"),
    SparkConsumer("SocketThread");

    private String logFileName;

    LogFileName(String fileName) {
        this.logFileName = fileName;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    public static LogFileName getAwardTypeEnum(String value) {
        LogFileName[] arr = values();
        for(LogFileName item : arr) {
            if(null != item && StringUtils.isNotBlank(item.logFileName)) {
                return item;
            }
        }
        return null;
    }
}
