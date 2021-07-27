package cn.xhjava.greenplum;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author Xiahu
 * @create 2021-07-07
 */
public class GreenPlumHelper {
    static String url = "jdbc:postgresql://192.168.0.113:5432/xh";
    static String username = "gpadmin";
    static String password = "gpadmin";
    private static Connection connection;

    public static Connection getConn() {
        /*synchronized (GreenPlumHelper.class) {
            if (connection == null) {
                synchronized (GreenPlumHelper.class) {
                    buildConn();
                }
            }

            return connection;
        }*/
        return buildConn();
    }

    public static void main(String[] args) throws SQLException {
        getConn();
        Statement statement = connection.createStatement();
        statement.execute("truncate table xh_test_1_import");
        connection.close();
    }

    private static Connection buildConn() {
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static final String DEFAULT_FIELD_DELIM = "\001";
    private static List<String> partitionList = Arrays.asList("2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020");
    private static Random random = new Random();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
    private static String temple = "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s" + DEFAULT_FIELD_DELIM +
            "%s";

    public static String buildData(int id) {
        return String.format(temple,
                id,
                partitionList.get(random(random, 0, partitionList.size())),
                "aa",
                "bb",
                "cc",
                "dd",
                "ee",
                "0",
                sdf.format(new Date()),
                id
        );
    }

    public static int random(Random random, int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }
}
