package com.clbr.dataxtest.kudu.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * @author XIAHU
 * @create 2019/8/30
 */

public class ColumnProp {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnProp.class);
    private static Properties prop = new Properties();
    private static Random random = new Random();

    /**
     * 生成随机的4--7个字母,组成表字段
     *
     * @return
     */
    public static String produceQualifier() {
        //随机产生字段值
        StringBuffer sb = new StringBuffer();
        int j = random.nextInt(7);
        if (j < 4) {
            j = 4;
        }
        for (int k = 0; k < j; k++) {
            char c = (char) (int) (Math.random() * 26 + 97);
            sb.append(c);
        }
        return sb.toString();
    }


    /**
     * 从properties中加载内容
     * @return
     */
    public static Column getColumn() {
        Column column = new Column();
        try {
            prop.load(Prop.class.getClassLoader().getResourceAsStream("columnName.properties"));
            column.setColumn_1(prop.getProperty("cloumn_1"));
            column.setColumn_2(prop.getProperty("cloumn_2"));
            column.setColumn_3(prop.getProperty("cloumn_3"));
            column.setColumn_4(prop.getProperty("cloumn_4"));
            column.setColumn_5(prop.getProperty("cloumn_5"));
            column.setColumn_6(prop.getProperty("cloumn_6"));
            column.setColumn_7(prop.getProperty("cloumn_7"));
            column.setColumn_8(prop.getProperty("cloumn_8"));
            column.setColumn_9(prop.getProperty("cloumn_9"));
            column.setColumn_10(prop.getProperty("cloumn_10"));
            column.setColumn_11(prop.getProperty("cloumn_11"));
            column.setColumn_12(prop.getProperty("cloumn_12"));
            column.setColumn_13(prop.getProperty("cloumn_13"));
            column.setColumn_14(prop.getProperty("cloumn_14"));
            column.setColumn_15(prop.getProperty("cloumn_15"));
            column.setColumn_16(prop.getProperty("cloumn_16"));
            column.setColumn_17(prop.getProperty("cloumn_17"));
            column.setColumn_18(prop.getProperty("cloumn_18"));
            column.setColumn_19(prop.getProperty("cloumn_19"));
        } catch (IOException e) {
            LOG.error(String.format("columnName.properties 读取异常---->%s", e.getMessage()));
        }
        return column;
    }


}