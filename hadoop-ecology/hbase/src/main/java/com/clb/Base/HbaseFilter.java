package com.clb.Base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseFilter {

    private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
    private static final String ZK_CONNECT_VALUE = "master:2181,node1:2181,node2:2181";

    private static Connection conn = null;
    private static Admin admin = null;
    private Scan scan = null;
    private Table table = null;


    @Before
    public void testBefor() throws IOException {
        //获取hbase连接
        Configuration conf = HBaseConfiguration.create();
        conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
        table = conn.getTable(TableName.valueOf("student"));
        scan = new Scan();
    }

    //行键过滤器 RowFilter
    public void testRowKeyFilter() throws IOException {


        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator("95007".getBytes()));
        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }


    //列族过滤器
    @Test
    public void testFamilyColumnFilter() throws IOException {
        Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("info".getBytes()));
        scan.setFilter(familyFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }


    //列过滤器
    public void testColumnFilter() throws IOException {
        Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
        scan.setFilter(qualifierFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }

    //值过滤器
    public void testValueFilter() throws IOException {
        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("男"));
        scan.setFilter(valueFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }


    //时间戳过滤器
    public void testTimeStampFilter() throws IOException {

        List<Long> list = new ArrayList<Long>();
        list.add(1522469029503l);
        TimestampsFilter timestampsFilter = new TimestampsFilter(list);
        scan.setFilter(timestampsFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier())
                        + "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
            }
        }
    }

    //单列值过滤器 SingleColumnValueFilter ----会返回满足条件的整行
    @Test
    public void testSingleColumnValueFilter() throws IOException {
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                "info".getBytes(),
                "name".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("刘晨"));
        singleColumnValueFilter.setFilterIfMissing(true);

        scan.setFilter(singleColumnValueFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier())
                        + "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
            }
        }
    }


    public void testSingleColumnValueExcludeFilter() throws IOException {
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(
                "info".getBytes(),
                "name".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("刘晨"));
        singleColumnValueExcludeFilter.setFilterIfMissing(true);

        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier())
                        + "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
            }
        }
    }


    //前缀过滤器 PrefixFilter----针对行键
    public void testPrefixFilter() throws IOException {

        PrefixFilter prefixFilter = new PrefixFilter("9501".getBytes());
        scan.setFilter(prefixFilter);


        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier())
                        + "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
            }
        }

    }

    //列前缀过滤器 ColumnPrefixFilter
    public void testColumnPrefixFilter() throws IOException {

        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter("name".getBytes());

        scan.setFilter(columnPrefixFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier())
                        + "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
            }
        }

    }


}