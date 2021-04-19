package com.clb.util;

import com.clb.domain.*;
import com.clb.pojo.OggMsg;

import java.util.*;

/**
 * @author Xiahu
 * @create 2020/12/9
 */
public class UmsTool {
    public static UMS getUMS(OggMsg oggMsg, String namespace) {
        UMS result = new UMS();
        result.setProtocol(new Protocol("data_increment_data"));
        Schema schema = new Schema();
        List<Payload> payloadList = new ArrayList<>();
        schema.setNamespace(namespace);
        LinkedList<Fields> fields = new LinkedList<>();
        LinkedList<String> tuple = new LinkedList<>();
        Map<String, String> after = oggMsg.getAfter();
        List<String> pkList = oggMsg.getPrimary_keys();
        for (String pk : pkList) {
            //TODO 联合主键无法解析
            String s = after.get(pk);
        }


        fields.addLast(new Fields("ums_id_", "long", false));
        tuple.addLast("pk");
        fields.addLast(new Fields("ums_ts_", "datetime", false));
        tuple.addLast(oggMsg.getCurrent_ts());
        fields.addLast(new Fields("ums_op_", "string", false));
        tuple.addLast(oggMsg.getOp_type());
        Iterator<Map.Entry<String, String>> entryIterator = after.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            fields.addLast(new Fields(entry.getKey(), "string", false));
            tuple.addLast(entry.getValue());
        }
        schema.setFields(fields);
        payloadList.add(new Payload(tuple));
        result.setSchema(schema);
        result.setPayload(payloadList);
        return result;
    }
}
