package com.clbr.dataxtest.kudu.core.curd;


import com.clbr.dataxtest.kudu.util.Column;
import com.clbr.dataxtest.kudu.util.ColumnProp;
import com.clbr.dataxtest.kudu.util.Prop;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @author XIAHU
 * @create 2019/9/6
 */

public class InsertTable {
    private static final Logger LOG = LoggerFactory.getLogger(InsertTable.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
    private static final int bufferSpace = 100000;//阈值
    private static Column column = ColumnProp.getColumn();
    private static Prop prop = new Prop();

    /**
     * 手动提交
     * @param startId
     * @param endId
     * @throws KuduException
     */
    public static void insertByManual(int startId, int endId) throws KuduException {
        //核心API(获取kudu的连接)
        KuduClient client = new KuduClient.KuduClientBuilder(prop.getConnInfo()).defaultOperationTimeoutMs(600000).build();
        KuduSession session = client.newSession();
        KuduTable table = client.openTable(prop.getKuduTable());


        //声明刷新模式
        SessionConfiguration.FlushMode flush = SessionConfiguration.FlushMode.MANUAL_FLUSH;
        session.setFlushMode(flush);
        //声明刷新阈值
        session.setMutationBufferSpace(bufferSpace);

        //开始插入数据
        int count = 0;
        for (int i = startId; i <= endId; i++) {
            //插入
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            String id = String.valueOf(i);
            //添加字段
            row.addString(column.getColumn_1(), id);
            row.addString(column.getColumn_2(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_3(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_4(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_5(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_6(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_7(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_8(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_9(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_10(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_11(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_12(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_13(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_14(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_15(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_16(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_17(), String.valueOf(0));
            row.addString(column.getColumn_18(), sdf.format(new Date()));
            row.addString(column.getColumn_19(), id);
            session.apply(insert);

            // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
            count++;
            if (count > bufferSpace / 2) {
                session.flush();
                count = 0;
            }
        }
        //最终提交一次,保证不漏数据
        if (count > 0) {
            session.flush();
        }
    }


    /**
     * 自动提交
     * @param startID
     * @param endId
     * @throws KuduException
     */
    public static void insertByAutu(int startID, int endId) throws KuduException {
        //核心API(获取kudu的连接)
        KuduClient client = new KuduClient.KuduClientBuilder(prop.getConnInfo()).defaultOperationTimeoutMs(600000).build();
        KuduSession session = client.newSession();
        KuduTable table = client.openTable(prop.getKuduTable());


        //声明刷新模式
        SessionConfiguration.FlushMode flush = SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
        session.setFlushMode(flush);
        //声明刷新阈值
        session.setMutationBufferSpace(bufferSpace);

        //开始插入数据
        int count = 0;
        for (int i = startID; i <= endId; i++) {
            //插入
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            //添加字段
            String id = UUID.randomUUID().toString();
            row.addString(column.getColumn_1(), id);
            row.addString(column.getColumn_2(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_3(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_4(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_5(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_6(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_7(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_8(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_9(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_10(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_11(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_12(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_13(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_14(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_15(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_16(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_17(), String.valueOf(0));
            row.addString(column.getColumn_18(), sdf.format(new Date()));
            row.addString(column.getColumn_19(), id);

            //对于AUTO_FLUSH_SYNC模式, apply()将立即完成kudu写入
            session.apply(insert);
        }
    }


    /**
     * 手动 + 自动刷新
     *
     * @param startID
     * @param endId
     * @throws Exception
     */
    public static void insertByAll(int startID, int endId) throws Exception {
        //核心API(获取kudu的连接)
        KuduClient client = new KuduClient.KuduClientBuilder(prop.getConnInfo()).defaultOperationTimeoutMs(600000).build();
        KuduSession session = client.newSession();
        KuduTable table = client.openTable(prop.getKuduTable());


        //声明刷新模式
        SessionConfiguration.FlushMode flush = SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND;
        session.setFlushMode(flush);
        //声明刷新阈值
        session.setMutationBufferSpace(bufferSpace);

        //开始插入数据
        int count = 0;
        for (int i = startID; i <= endId; i++) {
            //插入
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            //添加字段
            String id = String.valueOf(i);
            row.addString(column.getColumn_1(), id);
            row.addString(column.getColumn_2(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_3(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_4(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_5(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_6(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_7(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_8(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_9(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_10(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_11(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_12(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_13(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_14(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_15(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_16(), ColumnProp.produceQualifier());
            row.addString(column.getColumn_17(), String.valueOf(0));
            row.addString(column.getColumn_18(), sdf.format(new Date()));
            row.addString(column.getColumn_19(), id);


            session.apply(insert);


            // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
            if (SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
                count = count + 1;
                if (count > bufferSpace / 2) {
                    session.flush();
                    count = 0;
                }
            }


        }
        // 对于手工提交, 保证完成最后的提交
        if (SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND && count > 0) {
            session.flush();
        }

        // 对于后台自动提交, 必须保证完成最后的提交, 并保证有错误时能抛出异常
        if (SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
            session.flush();
            RowErrorsAndOverflowStatus error = session.getPendingErrors();
            if (error.isOverflowed() || error.getRowErrors().length > 0) {
                if (error.isOverflowed()) {
                    throw new Exception("Kudu overflow exception occurred.");
                }
                StringBuilder errorMessage = new StringBuilder();
                if (error.getRowErrors().length > 0) {
                    for (RowError errorObj : error.getRowErrors()) {
                        errorMessage.append(errorObj.toString());
                        errorMessage.append(";");
                    }
                }
                throw new Exception(errorMessage.toString());
            }
        }
    }

}