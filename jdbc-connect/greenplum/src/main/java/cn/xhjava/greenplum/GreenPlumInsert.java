package cn.xhjava.greenplum;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Xiahu
 * @create 2021-07-07
 */
@Slf4j
public class GreenPlumInsert implements Runnable {

    private static final String DEFAULT_FIELD_DELIM = "\001";
    private static final String DEFAULT_NULL_DELIM = "\002";
    private static final String LINE_DELIMITER = "\n";
    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s'";
    private static double MB = 1024 * 1024.0;
    private static int batch_size = 100000;

    private String targetTable;
    private String[] fieldNames;
    private CountDownLatch cdl;
    private int insertNum;
    private CopyManager copyManager;

    private long totalBytes;


    public GreenPlumInsert(String targetTable, String[] fieldNames, int insertNum, CountDownLatch cdl) {
        this.targetTable = targetTable;
        this.fieldNames = fieldNames;
        this.insertNum = insertNum;
        this.cdl = cdl;
        try {
            copyManager = new CopyManager((BaseConnection) GreenPlumHelper.getConn());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        int count = 0;
        StringBuffer dataBuff = new StringBuffer();
        for (int i = 1; i <= insertNum; i++) {
            count++;
            //封装BufferData
            dataBuff.append(GreenPlumHelper.buildData(i));
            dataBuff.append(LINE_DELIMITER);
            if (count >= batch_size) {
                insert(dataBuff, count);
                log.debug(String.format("%s --> 已经写入%s", targetTable, batch_size));
                dataBuff = new StringBuffer();
                count = 0;
            }
        }

        if (count > 0) {
            insert(dataBuff, count);
            dataBuff = new StringBuffer();
            count = 0;
        }
        long endTime = System.currentTimeMillis();

        log.info(String.format("%s -> %s 数据全部写入,耗费时间： %s s ,数据大小:  %s MB", targetTable, insertNum, (endTime - startTime) / 1000.0, (totalBytes / MB)));
        cdl.countDown();
    }


    private void insert(StringBuffer dataBuff, int count) {
        totalBytes += dataBuff.toString().getBytes().length;
        ByteArrayInputStream bi = new ByteArrayInputStream(dataBuff.toString().getBytes(StandardCharsets.UTF_8));
        String copysql = String.format(COPY_SQL_TEMPL, targetTable, "id,fk_id,qfxh,nioroa,gwvz,joqtf,isdeleted,lastupdatedttm,rowkey", GreenPlumHelper.DEFAULT_FIELD_DELIM, DEFAULT_NULL_DELIM);
        try {
            copyManager.copyIn(copysql, bi);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
