package com.clb.hoodie;

/**
 * @author Xiahu
 * @create 2020/9/21
 */
public enum PartitionStyle {
    HASH("HASH"), YARN("YYYY"), MONTH("YYYY-MM"), DAY("YYYY-MM-DD");


    private String style;

    PartitionStyle(String style) {
        this.style = style;
    }

    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }}
