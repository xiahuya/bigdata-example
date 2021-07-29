package cn.xhjava.connect.pool.domain;

/**
 * @author Xiahu
 * @create 2021-07-29
 */
public class ConnectBean {
    private String connType;
    private String connDriver;
    private String connJdbcUrl;
    private String connUsername;
    private String connPassword;
    private Integer connPoolSize;

    public ConnectBean() {
    }

    public ConnectBean(String connType, String connDriver, String connJdbcUrl, String connUsername, String connPassword, Integer connPoolSize) {
        this.connType = connType;
        this.connDriver = connDriver;
        this.connJdbcUrl = connJdbcUrl;
        this.connUsername = connUsername;
        this.connPassword = connPassword;
        this.connPoolSize = connPoolSize;
    }

    public String getConnType() {
        return connType;
    }

    public void setConnType(String connType) {
        this.connType = connType;
    }

    public String getConnDriver() {
        return connDriver;
    }

    public void setConnDriver(String connDriver) {
        this.connDriver = connDriver;
    }

    public String getConnJdbcUrl() {
        return connJdbcUrl;
    }

    public void setConnJdbcUrl(String connJdbcUrl) {
        this.connJdbcUrl = connJdbcUrl;
    }

    public String getConnUsername() {
        return connUsername;
    }

    public void setConnUsername(String connUsername) {
        this.connUsername = connUsername;
    }

    public String getConnPassword() {
        return connPassword;
    }

    public void setConnPassword(String connPassword) {
        this.connPassword = connPassword;
    }

    public Integer getConnPoolSize() {
        return connPoolSize;
    }

    public void setConnPoolSize(Integer connPoolSize) {
        this.connPoolSize = connPoolSize;
    }
}
