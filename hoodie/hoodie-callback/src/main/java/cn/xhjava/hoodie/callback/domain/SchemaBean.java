package cn.xhjava.hoodie.callback.domain;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Xiahu
 * @create 2021-05-21
 */
public class SchemaBean {

    private String type;
    private String name;
    private LinkedList<Fields> fields;

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setFields(LinkedList<Fields> fields) {
        this.fields = fields;
    }

    public List<Fields> getFields() {
        return fields;
    }

}

