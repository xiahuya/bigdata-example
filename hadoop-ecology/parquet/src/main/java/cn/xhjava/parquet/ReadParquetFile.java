package cn.xhjava.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author Xiahu
 * @create 2021/4/1
 */
public class ReadParquetFile {

    /**
     * 读取parquet文件的schema
     *
     * @param path
     * @throws IOException
     */
    public static void readParquetSchema(String path) throws IOException {
        Configuration configuration = new Configuration();
        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, new Path(path));
        MessageType schema = parquetMetadata.getFileMetaData().getSchema();
        System.out.println(schema);
    }

    public static void readParquetData(String path) throws IOException {
        StringBuffer sb = new StringBuffer();
        ParquetReader reader = new ParquetReader(new Path(path), new GroupReadSupport());
        for (Object value = reader.read(); value != null; value = reader.read()) {
            sb.append(value);
            sb.append("\r\n");
        }
        System.out.println(sb.toString());

    }

    public static void main(String[] args) {
        try {
            //ReadParquetFile.readParquetSchema("/tmp/a.parquet");
            ReadParquetFile.readParquetData("/tmp/a.parquet");
        } catch (Exception e) {

        }
    }
}
