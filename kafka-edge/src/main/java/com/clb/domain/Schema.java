package com.clb.domain;

import java.util.List;

/**
 * @author Xiahu
 * @create 2020/12/9
 */
public class Schema {
    private String namespace;
    private List<Fields> fields;
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    public String getNamespace() {
        return namespace;
    }

    public void setFields(List<Fields> fields) {
        this.fields = fields;
    }
    public List<Fields> getFields() {
        return fields;
    }
}
