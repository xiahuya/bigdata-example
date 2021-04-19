package cn.xhjava.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021/4/1
 */
public class WriteParquetFile {

    public static void writeParquet() throws IOException {
        //1.封装parquet Schema
        StringBuffer sb = new StringBuffer("message Pair {\n");
        sb.append(" required BINARY " + "id" + " (UTF8);\n");
        sb.append(" required BINARY " + "name" + " (UTF8);\n");
        sb.append(" required BINARY " + "sex" + " (UTF8);\n");
        sb.append(" required BINARY " + "brithday" + " (UTF8);\n");
        sb.append("}");
        //System.out.println(sb.toString());
        //2.生成Schema
        MessageType schema = MessageTypeParser.parseMessageType(sb.toString());
        //数据工厂
        GroupFactory factory = new SimpleGroupFactory(schema);
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema, configuration);

        //3.初始化parquet write
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                new Path("/tmp/a.parquet"),
                ParquetFileWriter.Mode.OVERWRITE,
                writeSupport,
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                configuration
        );
        //4.添加数据
        Group group = factory.newGroup();
        group.append("id", "1");
        group.append("name", "张三");
        group.append("sex", "男");
        group.append("brithday", "2020-01-01");
        //5.写入数据
        writer.write(group);
        Group group2 = factory.newGroup();
        group2.append("id", "2");
        group2.append("name", "张三2");
        group2.append("sex", "男2");
        group2.append("brithday", "2020-01-02");
        writer.write(group2);
        writer.close();
    }

    public static void main(String[] args) {
        try {
            WriteParquetFile.writeParquet();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
