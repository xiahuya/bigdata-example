package cn.xhjava.hoodie.callback.util;

import cn.xhjava.hoodie.callback.domain.Fields;
import cn.xhjava.hoodie.callback.domain.SchemaBean;
import com.alibaba.fastjson.JSON;
import org.apache.avro.Schema;

import java.util.LinkedList;

/**
 * @author Xiahu
 * @create 2021-05-21
 * <p>
 * Schema 的封装,传入自定义参数,返回Schema
 */
public class SchemaUtil {
//        public static Schema schemaParse(String[] fieldNames, TypeInformation<?>[] fieldTypes, String tableName) {
    public static Schema schemaParse(String[] fieldNames, String[] fieldTypes, String tableName) {
        SchemaBean schema = new SchemaBean();
        schema.setType("record");
        schema.setName(tableName);
        LinkedList<Fields> fieldList = new LinkedList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            Fields field = new Fields();
            field.setName(fieldNames[i].trim());
            String fieldType = fieldTypes[i].toString();
            if (fieldType.toLowerCase().startsWith("varchar")) {
                field.setType("string");
            } else if (fieldType.toLowerCase().startsWith("int")) {
                field.setType("int");
            } else if (fieldType.toLowerCase().startsWith("bigint")) {
                field.setType("int");
            } else {
                field.setType("string");
            }

            fieldList.addLast(field);
        }
        schema.setFields(fieldList);

        return new Schema.Parser().parse(JSON.toJSONString(schema));
    }

    public static void main(String[] args) {
        String[] fieldName = {"id", "fk_id", " qfxh", " jdpj", " nioroa", " gwvz", " joqtf", " isdeleted", " lastupdatedttm", " rowkey"};
        String[] fieldType = {"varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar"};
//        RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(SchemaUtil.schemaParse(fieldName, fieldType, "xh.test")).getLogicalType();
    }
}
