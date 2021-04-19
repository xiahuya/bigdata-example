package com.clb.domain;


import java.util.List;

/**
 * @author Xiahu
 * @create 2020/12/9
 */
public class UMS {
    private Protocol protocol;
    private Schema schema;
    private List<Payload> payload;
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }
    public Protocol getProtocol() {
        return protocol;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
    public Schema getSchema() {
        return schema;
    }

    public void setPayload(List<Payload> payload) {
        this.payload = payload;
    }
    public List<Payload> getPayload() {
        return payload;
    }
}
