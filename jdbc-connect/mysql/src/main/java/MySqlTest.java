import cn.xhjava.connect.pool.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * @author Xiahu
 * @create 2021/10/19 0019
 */
@Slf4j
public class MySqlTest {
    private static String sql = "insert into hid0101_his_cache_xh.test_%s(id, name, other_id, sex1, sex2, sex3, sex4, sex5, sex6, sex7)values(%s, '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
    private static Random random = new Random();
    private static String sex = "Aa_";
    private static int count = 1000;

    public static void main(String[] args) throws SQLException {
        ConnectionPool connectionPool = new ConnectionPool();
        Statement statement = connectionPool.getConnection().createStatement();
        log.info("开始更新数据");
        for (int j = 0; j < count; j++) {
            for (int i = 1; i <= 10; i++) {
                String insertsql = String.format(sql, i, j, "zhangsan_" + i, getRandomNumber(random, 0, 1000), sex + j, sex + j, sex + j, sex + j, sex + j, sex + j, sex + j);
                statement.execute(insertsql);
            }
        }
        log.info("结束更新数据");
    }


    private static int getRandomNumber(Random random, int min, int max) {
        return random.nextInt(max) % (max - min + 1) + min;
    }


    private Connection connection;

    @Before
    public void init() {
        ConnectionPool connectionPool = new ConnectionPool();
        connection = connectionPool.getConnection();
    }

    @Test
    public void insertData() throws SQLException {
        int count = 10000000;
        connection.setAutoCommit(false);
        int index = 0;
        String sql = "insert into hid0101_his_cache_xh.big_data(id, name, other_id, sex1, sex2, sex3, sex4, sex5, sex6, sex7)values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 1; i <= count; i++) {
            statement.setInt(1, i);
            statement.setString(2, "zhangsan_" + i);
            statement.setInt(3, getRandomNumber(random, 0, 1000));
            statement.setString(4, sex + i);
            statement.setString(5, sex + i);
            statement.setString(6, sex + i);
            statement.setString(7, sex + i);
            statement.setString(8, sex + i);
            statement.setString(9, sex + i);
            statement.setString(10, sex + i);
            statement.addBatch();
            index++;
            if (index > 10000) {
                statement.executeBatch();
                connection.commit();
                log.info("insert {}  data", index);
                index = 0;
            }
        }
        statement.executeBatch();
        connection.commit();
    }
}
