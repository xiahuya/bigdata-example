package com.clbr.dataxtest.kudu.core.curd;


/**
 * @author XIAHU
 * @create 2019/9/6
 */

import com.clbr.dataxtest.kudu.core.connect.Impala;
import com.clbr.dataxtest.kudu.util.Prop;
import org.apache.kudu.client.*;

/**
 * 删除kudu
 */
public class DeleteTable {
    private static Prop prop = new Prop();
    private static Impala impala = new Impala();

    /**
     * 删除表
     */
    public static void deleteTable() throws KuduException {
        //1.删除impala中的表
        impala.updataTable("DROP TABLE " + prop.getImpalaTableName());
        //删除kudu表
        final String masteraddr = prop.getConnInfo();
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).build();
        client.deleteTable(prop.getKuduTable());
        client.close();
    }


    /**
     * 删除指定行
     *
     * @throws KuduException
     */
    public static void deleteById(String id) throws KuduException {
        // master地址
        String masteraddr = prop.getConnInfo();
        // 创建kudu的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(60000).build();
        // 打开表
        KuduTable table = client.openTable(prop.getKuduTable());
        // 创建写session,kudu必须通过session写入
        KuduSession session = client.newSession();
        final Delete delete = table.newDelete();
        delete.getRow().addString("id", id);
        session.flush();
        session.apply(delete);
        session.close();
        client.close();

    }

}