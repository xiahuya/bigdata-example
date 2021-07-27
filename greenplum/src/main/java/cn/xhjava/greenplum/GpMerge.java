package cn.xhjava.greenplum;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author Xiahu
 * @create 2021-07-07
 */
public class GpMerge {
    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s' NULL as '%s'";
    private static final String DEFAULT_FIELD_DELIM = "\001";
    private static final String DEFAULT_NULL_DELIM = "\002";
    private static final String LINE_DELIMITER = "\n";


    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        String url = "jdbc:postgresql://192.168.0.113:6543/demo";
        String username = "gpadmin6";
        String password = "gpadmin";
        String table = "test_import";
        Class.forName("org.postgresql.Driver");
        long cacheByte = 1024 * 1024 * 50;
        int currentByte = 0;
        long totalByte = 0;
        int totalRecord = 0;
        Connection connection = DriverManager.getConnection(url, username, password);
        CopyManager copyManager = new CopyManager((BaseConnection) connection);
        Random random = new Random();
        StringBuffer stringBuffer = new StringBuffer();
        int num = 1000000;
        for (int i = 0; i < num; i++) {
            String name = "hj001";
            int age = random.nextInt(100);
            double d1 = random.nextDouble();
            String str1 = "201501020161_1_277_000000" + i;
            long createtime = System.currentTimeMillis();
            String str = i + DEFAULT_FIELD_DELIM
                    + name + DEFAULT_FIELD_DELIM
                    + age + DEFAULT_FIELD_DELIM
                    + d1 + DEFAULT_FIELD_DELIM
                    + str1 + DEFAULT_FIELD_DELIM
                    + createtime;
            totalRecord++;
            stringBuffer.append(str);
            currentByte += str.getBytes().length;
            if (totalRecord != num && currentByte < cacheByte) {
                stringBuffer.append(LINE_DELIMITER);
                continue;
            }
            ByteArrayInputStream bi = new ByteArrayInputStream(stringBuffer.toString().getBytes(StandardCharsets.UTF_8));
            String copysql = String.format(COPY_SQL_TEMPL, table, "id,name,age,d1,str1,createtime", DEFAULT_FIELD_DELIM, DEFAULT_NULL_DELIM);
            copyManager.copyIn(copysql, bi);
            totalByte = totalByte + currentByte;
            currentByte = 0;
            stringBuffer = new StringBuffer("");
        }
        long end = System.currentTimeMillis();
        System.out.println("执行时间:" + (end - start) / 1000.0);
        System.out.println("执行tps:" + num / ((end - start) / 1000));
        System.out.println("写入速度:" + totalByte / 1024 / 1024 / ((end - start) / 1000));
        System.out.println(totalByte);
        connection.close();
    }
}
