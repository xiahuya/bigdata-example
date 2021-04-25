package com.clb.Base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HbaseDML {
    Configuration configuration = null;
    Connection conn = null;
    Table table = null;

    @Before
    public void getConn() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node4");
        conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf("test_insertData"));
    }

    @Test
    public void printStr() {
        for (int i = 1; i <= 1000; i++) {
            StringBuffer sb = new StringBuffer();
            //int number = (int)(1+Math.random()*(10000000));
            sb.append(i).append(",").append("tom" + i).append(",").append("男");
            System.out.println(sb.toString());
        }
    }


    @Test
    public void testPutStructure() throws IOException {
        //Put put = new Put(Bytes.toBytes("1"));
        Get get = new Get(Bytes.toBytes("1"));
        Result result = table.get(get);
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            byte[] rowArray = cell.getRowArray();//rowkey
            byte[] familyArray = cell.getFamilyArray();//family
            byte[] qualifierArray = cell.getQualifierArray();//列
            byte[] values = cell.getValueArray();
            System.out.println("rowkey:" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
            System.out.println("columnFamily:" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
            System.out.println("qualifier:" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("value:" + new String(values, cell.getValueOffset(), cell.getValueLength()));
        }

    }


    /**
     * 增
     * 改:hbase不能修改数据,只能覆盖，用put
     */
    @Test
    public void testPut() throws Exception {
        //获取一个操作指定表的操作对象
        Table testHbase = conn.getTable(TableName.valueOf("testHbase"));
        //构造要插入对象为Put类型,一个Put对于一个rowKey
        Put put = new Put(Bytes.toBytes("001"));
        //添加要添加的内容，分别对于:columnFamily,key,value
        put.addColumn(Bytes.toBytes("city"), Bytes.toBytes("big"), Bytes.toBytes("上海"));
        put.addColumn(Bytes.toBytes("city"), Bytes.toBytes("small"), Bytes.toBytes("孝感"));
        put.addColumn(Bytes.toBytes("sex"), Bytes.toBytes("上海"), Bytes.toBytes("男"));
        put.addColumn(Bytes.toBytes("sex"), Bytes.toBytes("孝感"), Bytes.toBytes("女"));
        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("男"), Bytes.toBytes("big"));
        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("女"), Bytes.toBytes("small"));

        Put put2 = new Put(Bytes.toBytes("002"));
        //添加要添加的内容，分别对于:columnFamily,key,value
        put2.addColumn(Bytes.toBytes("city"), Bytes.toBytes("big"), Bytes.toBytes("上海"));
        put2.addColumn(Bytes.toBytes("city"), Bytes.toBytes("small"), Bytes.toBytes("孝感"));
        put2.addColumn(Bytes.toBytes("sex"), Bytes.toBytes("上海"), Bytes.toBytes("男"));
        put2.addColumn(Bytes.toBytes("sex"), Bytes.toBytes("孝感"), Bytes.toBytes("女"));
        put2.addColumn(Bytes.toBytes("name"), Bytes.toBytes("男"), Bytes.toBytes("big"));
        put2.addColumn(Bytes.toBytes("name"), Bytes.toBytes("女"), Bytes.toBytes("small"));

        ArrayList<Put> list = new ArrayList<Put>();
        list.add(put);
        list.add(put2);

        testHbase.put(list);

        testHbase.close();
        conn.close();

    }

    /**
     * 大量添加列
     *
     * @throws Exception
     */
    @Test
    public void testManyPuts() throws Exception {
        //获取一个操作指定表的操作对象
        for (int a = 32; a <= 45; a++) {
            Table testHbase = conn.getTable(TableName.valueOf("lookup:realtime_dim_" + a));
            ArrayList<Put> puts = new ArrayList<Put>();
            for (int i = 1; i < 500000; i++) {
                Put put = new Put(Bytes.toBytes(String.valueOf(i)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Harden_" + i));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(i)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("boy"));
                puts.add(put);

                if (puts.size() > 100000) {
                    testHbase.put(puts);
                    puts.clear();
                    System.out.println("成功插入hbase~");
                }
            }

            testHbase.put(puts);
            testHbase.close();
            System.out.println("******成功插入lookup:realtime_dim_" + a);
        }
        conn.close();


        //获取一个操作指定表的操作对象
        /*StringBuffer sb = new StringBuffer();
        for (int a = 1; a <= 45; a++) {
            String s = "lookup:realtime_dim_" + a;
            sb.append(s).append(",");
        }
        System.out.println(sb.toString());*/
    }

    /**
     * 删除rowkey(一行)
     *
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        Table testHbase = conn.getTable(TableName.valueOf("stu"));
        BufferedMutator mutator = conn.getBufferedMutator(TableName.valueOf("stu"));
        List<Mutation> mutations = new ArrayList<Mutation>();
        Delete delete = new Delete(Bytes.toBytes("0001"));
        //Delete delete2 = new Delete(Bytes.toBytes("002"));
        ArrayList<Delete> list = new ArrayList<Delete>();
        list.add(delete);
        //list.add(delete2);
        //testHbase.delete(list);

        Put put = new Put(Bytes.toBytes("0001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("small"), Bytes.toBytes("shanghai"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("yellow"), Bytes.toBytes("man"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("man"), Bytes.toBytes("jack"));
        mutations.add(delete);
        mutations.add(put);
        mutator.mutate(mutations);
        mutator.flush();
        mutations.clear();
        testHbase.close();
        conn.close();
    }

    /**
     * 根据rowKey获取信息,类似与(get 'tableName','rowkey')
     *
     * @throws IOException
     */
    @Test
    public void testSearch() throws IOException {
        Table testHbase = conn.getTable(TableName.valueOf("stu"));

        Get get = new Get(Bytes.toBytes("0001"));
        get.setMaxVersions(1)
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
//        get.setTimeRange(0L, 1603437485297L);
        Result result = testHbase.get(get);
//        System.out.println(result.getRow().toString());
//        //从结果中取出指定某个用户的value
//        byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("value"));
//        System.out.println(new String(value));
//        System.out.println("------------------------------------------");

//        //遍历整行结果中的所有k:v单元格
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            //当前的单元格
            Cell cell = cellScanner.current();
            byte[] rowArray = cell.getRowArray();//rowkey
            byte[] familyArray = cell.getFamilyArray();//family
            byte[] qualifierArray = cell.getQualifierArray();//列
            byte[] values = cell.getValueArray();
            System.out.println("rowkey:" + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
            System.out.println("columnFamily:" + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
            System.out.println("qualifier:" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("value:" + new String(values, cell.getValueOffset(), cell.getValueLength()));

        }
        System.out.println("----------");
        testHbase.close();
        conn.close();

    }

}