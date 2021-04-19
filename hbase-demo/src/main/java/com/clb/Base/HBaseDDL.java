package com.clb.Base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hbase.TableName.*;
import static org.apache.hadoop.hbase.TableName.valueOf;


public class HBaseDDL {
    Configuration configuration = null;
    Connection conn = null;

    @Before
    public void getConn() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.0.115");
        conn = ConnectionFactory.createConnection(configuration);

    }


    @Test
    //创建一张表
    public void creataTable() throws Exception {
        // 从连接中构造一个DDL操作器
        Admin admin = conn.getAdmin();
        //创建一个表定义描述对象
        HTableDescriptor tableName = new HTableDescriptor(valueOf("word"));

        //创建一个列族定义描述对象
        HColumnDescriptor columnFamily_1 = new HColumnDescriptor("content");
        //设置列族最大版本号
        columnFamily_1.setMaxVersions(3);
        //HColumnDescriptor columnFamily_2 = new HColumnDescriptor("sex");

        //将列族定义对象放入表定义对象中
        tableName.addFamily(columnFamily_1);
        //tableName.addFamily(columnFamily_2);

        //用DDL操作对象创建表
        admin.createTable(tableName);

        //关闭连接
        admin.close();
        conn.close();


    }

    @Test
    //删除表
    public void dropTable() throws IOException {
        //从连接构造一个DDL操作器
        Admin admin = conn.getAdmin();
        // 停用表
        admin.disableTable(TableName.valueOf("word"));
        //删除表
        admin.deleteTable(TableName.valueOf("word"));
        //关闭连接
        admin.close();
        conn.close();
    }

    @Test
    //修改表定义---添加一个列族
    public void alterTable() throws IOException {
        Admin admin = conn.getAdmin();
        //取出旧的表定义信息
        HTableDescriptor tableName = admin.getTableDescriptor(valueOf("testHbase"));
        //新构造一个列族定义
        HColumnDescriptor columnFamily = new HColumnDescriptor("name");
        //设置布隆过滤器类型
        columnFamily.setBloomFilterType(BloomType.ROWCOL);
        tableName.addFamily(columnFamily);

        //将修改过的表交给admin去提交
        admin.modifyTable(TableName.valueOf("testHbase"), tableName);

        //关闭连接
        admin.close();
        conn.close();

    }

}