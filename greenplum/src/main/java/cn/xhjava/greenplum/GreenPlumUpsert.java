package cn.xhjava.greenplum;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-07-07
 */
@Slf4j
public class GreenPlumUpsert implements Runnable {

    private static final String DEFAULT_FIELD_DELIM = "\001";
    private static final String DEFAULT_NULL_DELIM = "\002";
    private static final String LINE_DELIMITER = "\n";
    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s'";
    private static double MB = 1024 * 1024.0;
    private static int batch_size = 100;

    private String targetTable;
    private CountDownLatch cdl;
    private int upsertNum;
    private int loopNum;
    private CopyManager copyManager;
    private int currentLoopNum;

    private long totalBytes;
    private String targetTableImport;
    private Random random = new Random();
    private Connection connection;


    public GreenPlumUpsert(String targetTable, int upsertNum, int loopNum, CountDownLatch cdl) {
        this.targetTable = targetTable;
        this.targetTableImport = targetTable + "_import_tmp";
        this.loopNum = loopNum;
        this.upsertNum = upsertNum;
        this.cdl = cdl;
        connection = GreenPlumHelper.getConn();

        try {
            copyManager = new CopyManager((BaseConnection) connection);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        int count = 0;
        StringBuffer dataBuff = new StringBuffer();
        while (currentLoopNum < loopNum) {
            for (int i = 1; i <= upsertNum; i++) {
                count++;
                //封装BufferData
                dataBuff.append(GreenPlumHelper.buildData(i));
                dataBuff.append(LINE_DELIMITER);
                if (count >= batch_size) {
                    upsert(dataBuff);
                    log.debug(String.format("%s -->upsert success count : %s", targetTable, batch_size));
                    dataBuff = new StringBuffer();
                    count = 0;
                }
            }

            if (count > 0) {
                upsert(dataBuff);
                dataBuff = new StringBuffer();
                count = 0;
            }
            currentLoopNum++;
        }
        long endTime = System.currentTimeMillis();
        log.info(String.format("%s ->  总数据量:  %s 全部upsert结束  ,数据总大小:  %s MB ,耗费时间： %s s", targetTable, (upsertNum * loopNum), (totalBytes / MB), (endTime - startTime) / 1000.0));
        cdl.countDown();
    }

    private String delete_sql = "delete from %s as target using %s as source where (target.id=source.id)";
    private String insert_sql = "insert into %s select * from %s";
    private String truncate_sql = "truncate table %s";

    private void upsert(StringBuffer dataBuff) {
        long startTime = System.currentTimeMillis();
        Statement statement = null;
        try {
            statement = GreenPlumHelper.getConn().createStatement();

            //1. 清空import表
            statement.execute(String.format(truncate_sql, targetTableImport));

            //2. 将新增数据写入import表
            insertToImport(dataBuff);

            //3. 删除target表内与import存在冲突的数据
            //delete from xh_test_1 as target using xh_test_1_impoort as source where (target.id=source.id);
            statement.execute(String.format(delete_sql, targetTable, targetTableImport));

            //4. 将import 表内的数据添加到target表
            // insert into xh_test_1 select * from xh_test_1_import;
            statement.execute(String.format(insert_sql, targetTable, targetTableImport));

            long endTime = System.currentTimeMillis();
            log.info(String.format("%s --->此时merge数据量： %s ,当前批次数据大小： %s MB ,merge总数据大小: %s MB , 耗费时间： %s s", targetTable, batch_size, dataBuff.toString().getBytes().length / MB, (totalBytes / MB), (endTime - startTime) / 1000.0));
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }


    private void insertToImport(StringBuffer dataBuff) {
        totalBytes += dataBuff.toString().getBytes().length;
        ByteArrayInputStream bi = new ByteArrayInputStream(dataBuff.toString().getBytes(StandardCharsets.UTF_8));
        String copysql = String.format(COPY_SQL_TEMPL, targetTableImport, "id,fk_id,qfxh,nioroa,gwvz,joqtf,isdeleted,lastupdatedttm,rowkey", GreenPlumHelper.DEFAULT_FIELD_DELIM, DEFAULT_NULL_DELIM);
        try {
            copyManager.copyIn(copysql, bi);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
