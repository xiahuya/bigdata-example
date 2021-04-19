package com.clbr.dataxtest.kudu.core.curd;


import com.clbr.dataxtest.kudu.util.Column;
import com.clbr.dataxtest.kudu.util.ColumnProp;
import com.clbr.dataxtest.kudu.util.Prop;
import org.apache.kudu.client.*;

/**
 * @author XIAHU
 * @create 2019/9/6
 */

public class ScanTable {
    private static Prop prop = new Prop();
    private static Column column = ColumnProp.getColumn();


    /**
     * 查询表,根据ID查询一行
     */
    public static void scanTableById(String id) throws KuduException {
        //master地址
        String masteraddr = prop.getConnInfo();
        //创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(60000).build();
        //打开表
        KuduTable table = client.openTable(prop.getKuduTable());
        KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
        /**
         * 设置搜索的条件
         * 如果不设置，则全表扫描
         */
        KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("id"),
                KuduPredicate.ComparisonOp.EQUAL, id);
        builder.addPredicate(predicate);
        // 开始扫描
        KuduScanner scaner = builder.build();

        while (scaner.hasMoreRows()) {
            RowResultIterator iterator = scaner.nextRows();
            while (iterator.hasNext()) {
                StringBuffer sb = new StringBuffer();
                RowResult result = iterator.next();
                sb.append(result.getString(column.getColumn_1()) + "\t");
                sb.append(result.getString(column.getColumn_2()) + "\t");
                sb.append(result.getString(column.getColumn_3()) + "\t");
                sb.append(result.getString(column.getColumn_4()) + "\t");
                sb.append(result.getString(column.getColumn_5()) + "\t");
                sb.append(result.getString(column.getColumn_6()) + "\t");
                sb.append(result.getString(column.getColumn_7()) + "\t");
                sb.append(result.getString(column.getColumn_8()) + "\t");
                sb.append(result.getString(column.getColumn_9()) + "\t");
                sb.append(result.getString(column.getColumn_10()) + "\t");
                sb.append(result.getString(column.getColumn_11()) + "\t");
                sb.append(result.getString(column.getColumn_12()) + "\t");
                sb.append(result.getString(column.getColumn_13()) + "\t");
                sb.append(result.getString(column.getColumn_14()) + "\t");
                sb.append(result.getString(column.getColumn_15()) + "\t");
                sb.append(result.getString(column.getColumn_16()) + "\t");
                sb.append(result.getString(column.getColumn_17()) + "\t");
                sb.append(result.getString(column.getColumn_18()) + "\t");
                sb.append(result.getString(column.getColumn_19()) + "\t");
                System.out.println(sb.toString());
                sb = null;
            }
        }
        scaner.close();
        client.close();
    }

}