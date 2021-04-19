package com.clb.domain;

/**
 * @author Xiahu
 * @create 2020/12/9
 */
public class Fields {
    private String name;
    private String type;
    private boolean nullable;

    public Fields(String name, String type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return name;
    }

    public void setType(String type) {
        this.type = type;
    }
    public String getType() {
        return type;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
    public boolean getNullable() {
        return nullable;
    }
}
