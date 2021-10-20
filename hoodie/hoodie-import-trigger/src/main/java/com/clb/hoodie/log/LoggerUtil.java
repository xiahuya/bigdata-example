package com.clb.hoodie.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Xiahu
 * @create 2020/7/10
 */
public class LoggerUtil {
    public static <T> Logger logger(Class<T> clazz) {
        return LoggerFactory.getLogger(clazz);
    }

    /**
     * 打印到指定的文件下
     *
     * @param desc 日志文件名称
     * @return
     */
    public static Logger logger(LogFileName desc) {
        return LoggerFactory.getLogger(desc.getLogFileName());
    }
}