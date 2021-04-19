package com.clbr.dataxtest.kudu.core.curd;


import com.clbr.dataxtest.kudu.core.connect.Impala;
import com.clbr.dataxtest.kudu.util.Column;
import com.clbr.dataxtest.kudu.util.ColumnProp;
import com.clbr.dataxtest.kudu.util.Prop;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;

import java.util.LinkedList;
import java.util.List;

/**
 * @author XIAHU
 * @create 2019/9/6
 */

public class CreateTable {
    private static Prop prop = new Prop();
    private static Column column = ColumnProp.getColumn();
    private static Impala impala = new Impala();


    /**
     * 创建无分区表
     *
     * @throws KuduException
     */
    public static void createTable() throws KuduException {
        //1.创建kudu表
        final String masteraddr = prop.getConnInfo();
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();
        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn(column.getColumn_1(), Type.STRING, true));
        columns.add(newColumn(column.getColumn_2(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_3(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_4(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_5(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_6(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_7(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_8(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_9(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_10(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_11(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_12(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_13(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_14(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_15(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_16(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_17(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_18(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_19(), Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add(column.getColumn_1());
        //设置表的备份数
        options.setNumReplicas(1);
        //设置range分区
        options.setRangePartitionColumns(parcols);
        //设置hash分区和数量
        options.addHashPartitions(parcols, 3);
        try {
            client.createTable(prop.getKuduTable(), schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        client.close();
        //2.映射到impala
        String createSQL = prop.getCreateSQL();
        createSQL = createSQL.replace("{impalaName}", prop.getImpalaTableName());
        createSQL = createSQL.replace("{kuduName}", prop.getKuduTable());
        impala.updataTable(createSQL);
    }


    /**
     * Range Partitioning (范围分区)
     */
    public static void createRangePartitioningTable(int partitionCount) throws KuduException {
        //master地址
        final String master = prop.getConnInfo();
        final KuduClient client = new KuduClient.KuduClientBuilder(master).defaultSocketReadTimeoutMs(60000).build();
        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn(column.getColumn_1(), Type.STRING, true));
        columns.add(newColumn(column.getColumn_2(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_3(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_4(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_5(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_6(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_7(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_8(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_9(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_10(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_11(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_12(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_13(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_14(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_15(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_16(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_17(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_18(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_19(), Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        final CreateTableOptions options = new CreateTableOptions();
        //设置备份数
        options.setNumReplicas(1);
        //设置范围分区的分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add(column.getColumn_18());
        //设置按照哪个字段进行range分区
        options.setRangePartitionColumns(parcols);
        /**
         * 设置range的分区范围
         * 分区1：2019--2019
         * 分区2：2018--2018
         * 分区3：2017--2017
         * ........
         * 分区9：2010--2010
         * */
        int count = 2019;
        for (int i = 1; i <= partitionCount; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addString(column.getColumn_18(), String.valueOf(count));
            PartialRow upper = schema.newPartialRow();
            lower.addString(column.getColumn_18(), String.valueOf(count));
            options.addRangePartition(lower, upper);
        }

        try {
            client.createTable(prop.getKuduTable(), schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        client.close();
        //2.映射到impala
        String createSQL = prop.getCreateSQL();
        createSQL = createSQL.replace("{impalaName}", prop.getImpalaTableName());
        createSQL = createSQL.replace("{kuduName}", prop.getKuduTable());
        impala.updataTable(createSQL);
    }


    /**
     * Hash Partitioning(哈希分区)
     *
     * @throws KuduException
     */
    public static void createHashPartitioningTable() throws KuduException {
        //1.创建kudu表
        final String masteraddr = prop.getConnInfo();
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();
        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn(column.getColumn_1(), Type.STRING, true));
        columns.add(newColumn(column.getColumn_2(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_3(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_4(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_5(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_6(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_7(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_8(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_9(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_10(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_11(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_12(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_13(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_14(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_15(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_16(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_17(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_18(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_19(), Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
        // 设置表的replica备份和分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add(column.getColumn_1());
        //设置表的备份数
        options.setNumReplicas(1);
        //设置按照哪个字段进行Hash分区
        options.addHashPartitions(parcols, 6);
        //设置range分区
        options.setRangePartitionColumns(parcols);
        //设置hash分区和数量
        options.addHashPartitions(parcols, 3);
        try {
            client.createTable(prop.getKuduTable(), schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        client.close();
        //2.映射到impala
        String createSQL = prop.getCreateSQL();
        createSQL = createSQL.replace("{impalaName}", prop.getImpalaTableName());
        createSQL = createSQL.replace("{kuduName}", prop.getKuduTable());
        impala.updataTable(createSQL);
    }


    public static void createMultilevelPartitioningTable(int partitionCount) throws KuduException {
        //master地址
        final String master = prop.getConnInfo();
        final KuduClient client = new KuduClient.KuduClientBuilder(master).defaultSocketReadTimeoutMs(60000).build();
        // 设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn(column.getColumn_1(), Type.STRING, true));
        columns.add(newColumn(column.getColumn_2(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_3(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_4(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_5(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_6(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_7(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_8(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_9(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_10(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_11(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_12(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_13(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_14(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_15(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_16(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_17(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_18(), Type.STRING, false));
        columns.add(newColumn(column.getColumn_19(), Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        final CreateTableOptions options = new CreateTableOptions();
        //设置备份数
        options.setNumReplicas(1);
        //设置范围分区的分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add(column.getColumn_18());
        //设置按照哪个字段进行range分区
        options.setRangePartitionColumns(parcols);
        //设置Hash分区
        options.addHashPartitions(parcols , partitionCount);
        options.setRangePartitionColumns(parcols);
        /**
         * 设置range的分区范围
         * 分区1：2019--2019
         * 分区2：2018--2018
         * 分区3：2017--2017
         * ........
         * 分区9：2010--2010
         * */
        int count = 2019;
        for (int i = 1; i <= partitionCount; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addString(column.getColumn_18(), String.valueOf(count));
            PartialRow upper = schema.newPartialRow();
            lower.addString(column.getColumn_18(), String.valueOf(count));
            options.addRangePartition(lower, upper);
        }

        try {
            client.createTable(prop.getKuduTable(), schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        client.close();
        //2.映射到impala
        String createSQL = prop.getCreateSQL();
        createSQL = createSQL.replace("{impalaName}", prop.getImpalaTableName());
        createSQL = createSQL.replace("{kuduName}", prop.getKuduTable());
        impala.updataTable(createSQL);
    }

    private static ColumnSchema newColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }

}
