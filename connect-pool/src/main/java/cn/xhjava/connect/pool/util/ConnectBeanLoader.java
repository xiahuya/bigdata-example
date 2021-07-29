package cn.xhjava.connect.pool.util;

import cn.xhjava.connect.pool.domain.ConnectBean;

import java.io.IOException;
import java.util.Properties;

import static cn.xhjava.connect.pool.domain.ConnectPoolConstant.*;

/**
 * @author Xiahu
 * @create 2021-07-29
 */
public class ConnectBeanLoader {


    public static ConnectBean loadConnectBean() {
        Properties prop = null;
        ConnectBean connectBean = null;
        try {
            prop = new Properties();
            prop.load(ConnectBeanLoader.class.getClassLoader().getResourceAsStream("connPool.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (null != prop) {
            connectBean = new ConnectBean();
            connectBean.setConnType(prop.getProperty(CONN_TYPE));
            connectBean.setConnDriver(prop.getProperty(CONN_DRIVER));
            connectBean.setConnJdbcUrl(prop.getProperty(CONN_JDBCURL));
            connectBean.setConnUsername(prop.getProperty(CONN_USERNAME));
            connectBean.setConnPassword(prop.getProperty(CONN_PASSWORD));
            connectBean.setConnPoolSize(Integer.valueOf(prop.getProperty(CONN_SIZE)));
        }
        return connectBean;
    }
}
