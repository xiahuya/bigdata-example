package com.clbr.dataxtest.kudu.core.curd;


/**
 * @author XIAHU
 * @create 2019/9/9
 */

import com.clbr.dataxtest.kudu.util.Column;
import com.clbr.dataxtest.kudu.util.ColumnProp;
import com.clbr.dataxtest.kudu.util.Prop;
import com.clbr.dataxtest.kudu.util.RandomUtil;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 多个表做更新操作
 */
public class UpdataManyTable {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateTable.class);
    private static final int bufferSpace = 100000;
    private static Prop prop = new Prop();
    private static Column column = ColumnProp.getColumn();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy");

    public static void updateTable(String tableName, Integer updateDataCount) {
        // master地址
        final String masteraddr = prop.getConnInfo();
        ;
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultOperationTimeoutMs(600000).build();
        // 打开表
        KuduSession session = null;
        try {
            KuduTable table = client.openTable(tableName);
            session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            //声明刷新阈值
            session.setMutationBufferSpace(bufferSpace);

            int count = 0;
            for (int i = 1; i <= updateDataCount; i++) {
                //更新数据
                Update update = table.newUpdate();
                PartialRow row = update.getRow();
                //添加字段
                String id = String.valueOf(RandomUtil.getRandomNum());
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
                row.addString(column.getColumn_17(), String.valueOf(1));
                row.addString(column.getColumn_18(), sdf.format(new Date()));
                row.addString(column.getColumn_19(), id);
                session.apply(update);

                // 对于手工提交, 需要buffer在未满的时候flush,这里采用了buffer一半时即提交
                count++;
                if (count > bufferSpace / 2) {
                    session.flush();
                    count = 0;
                }
                LOG.info(String.format("Update ID is: %s" + id));
            }
            //最终提交一次,保证不漏数据
            if (count > 0) {
                session.flush();
            }
        } catch (KuduException e) {
            LOG.error(String.format(e.getMessage()));
        }
    }
}