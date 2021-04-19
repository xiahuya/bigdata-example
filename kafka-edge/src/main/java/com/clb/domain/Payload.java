package com.clb.domain;

import java.util.List;

/**
 * @author Xiahu
 * @create 2020/12/9
 */
public class Payload {
    private List<String> tuple;

    public Payload(List<String> tuple) {
        this.tuple = tuple;
    }

    public void setTuple(List<String> tuple) {
        this.tuple = tuple;
    }

    public List<String> getTuple() {
        return tuple;
    }

}
