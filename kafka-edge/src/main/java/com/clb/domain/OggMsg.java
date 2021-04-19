package com.clb.domain;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * @author Xiahu
 * @create 2020/6/12
 */
public class OggMsg implements Serializable {
    private String table;
    private String op_type;
    private String op_ts;
    private String current_ts;
    private String pos;
    private LinkedList<String> primary_keys;
    private LinkedHashMap<String,String> before;
    private LinkedHashMap<String,String> after;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    public String getOp_ts() {
        return op_ts;
    }

    public void setOp_ts(String op_ts) {
        this.op_ts = op_ts;
    }

    public String getCurrent_ts() {
        return current_ts;
    }

    public void setCurrent_ts(String current_ts) {
        this.current_ts = current_ts;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public LinkedList<String> getPrimary_keys() {
        return primary_keys;
    }

    public void setPrimary_keys(LinkedList<String> primary_keys) {
        this.primary_keys = primary_keys;
    }

    public LinkedHashMap<String, String> getBefore() {
        return before;
    }

    public void setBefore(LinkedHashMap<String, String> before) {
        this.before = before;
    }

    public LinkedHashMap<String, String> getAfter() {
        return after;
    }

    public void setAfter(LinkedHashMap<String, String> after) {
        this.after = after;
    }
}
