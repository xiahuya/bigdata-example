package cn.xhjava.hudi.flink.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.streamer.FlinkStreamerConfig;

/**
 * @author Xiahu
 * @create 2021-06-29
 */
public class HoodieFlinkCompact {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkStreamerConfig cfg = new FlinkStreamerConfig();
        cfg.propsFilePath = "/tmp/hudi-mor-config.properties";
        cfg.targetBasePath = "/tmp/xiahu/xh_mor";
        cfg.readSchemaFilePath = "/tmp/hoodie_table_sink.avsc";
        cfg.targetTableName = "xh_mor";
        cfg.tableType = "MERGE_ON_READ";
        cfg.recordKeyField = "rowkey";
        cfg.partitionPathField = "fk_id";
        cfg.sourceOrderingField = "lastupdatedttm";
        cfg.keygenClass = "org.apache.hudi.keygen.SimpleAvroKeyGenerator";

        Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);
        HoodieCompactHelper helper = new HoodieCompactHelper(conf);

        conf.setString(FlinkOptions.TABLE_NAME, helper.getMetaClient().getTableConfig().getTableName());

        //1. 回滚
        helper.rollbackCompaction();

        //2.生成调度计划,生成 xxx.compaction.requested 文件
        helper.scheduleCompaction();
//        helper.scheduleCompaction("20210629150543");

        //3.执行compact操作
        //helper.compact();
    }

    /**
     * hoodie compact 目前有三种方式:
     *  1.使用hudi 自己实现的类：org.apache.hudi.sink.compact.HoodieFlinkCompactor
     *    此类可以实现compact的所有功能,但是作为一个 long time 任务,无法在处理完数据后自己关闭程序,需要人为进行： yarn kill xxx
     *
     *  2.在java 端进行rollbackCompaction,scheduleCompaction, compact操作在Spark端进行
     *    弊端: 实现比较麻烦,Spark端的操作不清楚是否还有其他问题
     *
     *  3.使用 hudi-cli 脚本进行 compact操作 (此实现使用Spark 执行 compact,流程基本跟方式一一致)
     */
}
