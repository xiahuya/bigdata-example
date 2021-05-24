package cn.xhjava.hudi.flink;

import com.beust.jcommander.JCommander;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021-05-20
 */
public class FlinkHoodieStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
        cfg.recordKeyField = "rowkey";
        cfg.sourceOrderingField = "lastupdatedttm";
        cfg.partitionPathField = "fk_id";
        JCommander cmd = new JCommander(cfg, null, args);
        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }
        env.enableCheckpointing(cfg.checkpointInterval);
        env.getConfig().setGlobalJobParameters(cfg);
        // We use checkpoint to trigger write operation, including instant generating and committing,
        // There can only be one checkpoint at one time.
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        if (cfg.flinkCheckPointPath != null) {
            env.setStateBackend(new FsStateBackend(cfg.flinkCheckPointPath));
        }

        Properties kafkaProps = StreamerUtil.appendKafkaProps(cfg);

        // Read from kafka source
        RowType rowType = (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(cfg)).getLogicalType();
        Configuration conf = FlinkOptions.fromStreamerConfig(cfg);
        int numWriteTask = conf.getInteger(FlinkOptions.WRITE_TASKS);
        StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
                new StreamWriteOperatorFactory<>(conf);

        SingleOutputStreamOperator<RowData> source = env.addSource(new FlinkKafkaConsumer<>(
                cfg.kafkaTopic,
                new JsonRowDataDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        false,
                        true,
                        TimestampFormat.ISO_8601
                ), kafkaProps))
                .name("kafka_source")
                .uid("uid_kafka_source");
        source.printToErr();
                /*.map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class))
                // Key-by partition path, to avoid multiple subtasks write to a partition at the same time
                .keyBy(HoodieRecord::getPartitionPath)
                .transform(
                        "bucket_assigner",
                        TypeInformation.of(HoodieRecord.class),
                        new KeyedProcessOperator<>(new BucketAssignFunction<>(conf)))
                .uid("uid_bucket_assigner")
                // shuffle by fileId(bucket id)
                .keyBy(record -> record.getCurrentLocation().getFileId())
                .transform("hoodie_stream_write", null, operatorFactory)
                .uid("uid_hoodie_stream_write")
                .setParallelism(numWriteTask)
                .addSink(new CleanFunction<>(conf))
                .setParallelism(1)
                .name("clean_commits")
                .uid("uid_clean_commits");*/

        env.execute(cfg.targetTableName);
    }
}
