package com.clbr.dataxtest.kudu.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author XIAHU
 * @create 2019/8/30
 */

public class Prop {
    private static final Logger LOG = LoggerFactory.getLogger(Prop.class);
    private String connInfo;
    private String kuduTable;
    private String impalaTableName;
    private String createSQL;
    private Properties prop = new Properties();

    public Prop() {
        build();
    }

    private void build() {
        try {
            //prop.load(Prop.class.getClassLoader().getResourceAsStream("kudu.properties"));
            prop.load(new InputStreamReader(new FileInputStream("/home/KLBR/kudu.properties")));
            this.connInfo = prop.getProperty("kuduclient");
            this.kuduTable = prop.getProperty("kuduTableName");
            this.impalaTableName = prop.getProperty("impalaTableName");
            this.createSQL = prop.getProperty("createSQL");
        } catch (IOException e) {
            LOG.error(String.format("kudu.properties 读取异常---->%s", e.getMessage()));
        }
    }

    public String getKuduTable() {
        return kuduTable;
    }

    public String getImpalaTableName() {
        return impalaTableName;
    }

    public String getCreateSQL() {
        return createSQL;
    }

    public String getConnInfo() {
        return connInfo;
    }
}