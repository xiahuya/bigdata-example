package cn.xhjava.hoodie.callback.domain;

import java.io.Serializable;

/**
 * @author Xiahu
 * @create 2021-06-17
 */
public class HoodieCallbackMsg implements Serializable {

    private static final long serialVersionUID = -4128525069507358775L;
    private String commitTime;

    private String tableName;

    private String basePath;

    public HoodieCallbackMsg() {
    }

    public HoodieCallbackMsg(String commitTime, String tableName, String basePath) {
        this.commitTime = commitTime;
        this.tableName = tableName;
        this.basePath = basePath;
    }

    public String getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(String commitTime) {
        this.commitTime = commitTime;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
}
