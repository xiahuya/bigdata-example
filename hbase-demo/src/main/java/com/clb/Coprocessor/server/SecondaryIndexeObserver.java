package com.clb.Coprocessor.server;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 〈二级索引表,在A表添加数据的时候,往索引表中同时添加一条另外的数据〉
 */
public class SecondaryIndexeObserver extends BaseRegionObserver {

    private RegionCoprocessorEnvironment env;
    private String secondaryIndexeTable = "";
    static Table table = null;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
        String hbasetable = env.getRegionInfo().getTable().getNameAsString();
        secondaryIndexeTable = hbasetable + "_INDEX";
        table = e.getTable(TableName.valueOf(secondaryIndexeTable));
    }

    /**
     * 此方法是在真正的put方法调用之前进行调用
     * A表在进行put操作时,同时对B表进行put操作
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        //获取put对象里面的rowkey'ergouzi'
        byte[] row = put.getRow();
        //获取put对象里面的cell
        List<Cell> list = put.get(Bytes.toBytes("info"), Bytes.toBytes("op_ts"));
        Cell cell = list.get(0);
        //创建一个新的put对象
        Put new_put = new Put(Bytes.toBytes(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
        new_put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rowKey"), Bytes.toBytes(new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())));
        table.put(new_put);
    }


}