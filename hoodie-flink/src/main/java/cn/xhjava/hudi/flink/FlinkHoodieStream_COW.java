package cn.xhjava.hudi.flink;

import com.beust.jcommander.JCommander;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.bootstrap.BootstrapFunction;
import org.apache.hudi.sink.compact.*;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketAssignOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import java.util.Properties;

/**
 * @author Xiahu
 * @create 2021-06-21
 */
public class FlinkHoodieStream_COW {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
        JCommander cmd = new JCommander(cfg, null, args);
        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }
        env.setParallelism(1);
        env.enableCheckpointing(cfg.checkpointInterval);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setGlobalJobParameters(cfg);
        // We use checkpoint to trigger write operation, including instant generating and committing,
        // There can only be one checkpoint at one time.
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        if (cfg.flinkCheckPointPath != null) {
            env.setStateBackend(new FsStateBackend(cfg.flinkCheckPointPath));
        }

        Properties kafkaProps = StreamerUtil.appendKafkaProps(cfg);

        // Read from kafka source
        RowType rowType =
                (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(cfg))
                        .getLogicalType();
        Configuration conf = FlinkStreamerConfig.toFlinkConfig(cfg);
        int numWriteTask = conf.getInteger(FlinkOptions.WRITE_TASKS);
        StreamWriteOperatorFactory<HoodieRecord> operatorFactory =
                new StreamWriteOperatorFactory<>(conf);


        DataStreamSource<RowData> source = env.addSource(new FlinkKafkaConsumer<>(
                cfg.kafkaTopic,
                new JsonRowDataDeserializationSchema(
                        rowType,
                        InternalTypeInfo.of(rowType),
                        false,
                        true,
                        TimestampFormat.ISO_8601
                ), kafkaProps));

        //source.printToErr("xiahu");

        DataStream<HoodieRecord> hoodieDataStream = source
                .name("kafka_source")
                .uid("uid_kafka_source")
                .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class));
        if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
            hoodieDataStream = hoodieDataStream.transform("index_bootstrap",
                    TypeInformation.of(HoodieRecord.class),
                    new ProcessOperator<>(new BootstrapFunction<>(conf)));
        }

        DataStream<Object> pipeline = hoodieDataStream
                // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
                .keyBy(HoodieRecord::getRecordKey)
                .transform(
                        "bucket_assigner",
                        TypeInformation.of(HoodieRecord.class),
                        new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
                .uid("uid_bucket_assigner")
                // shuffle by fileId(bucket id)
                .keyBy(record -> record.getCurrentLocation().getFileId())
                .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
                .uid("uid_hoodie_stream_write")
                .setParallelism(1);
        if (StreamerUtil.needsAsyncCompaction(conf)) {
            pipeline.transform("compact_plan_generate",
                    TypeInformation.of(CompactionPlanEvent.class),
                    new CompactionPlanOperator(conf))
                    .uid("uid_compact_plan_generate")
                    .setParallelism(1) // plan generate must be singleton
                    .rebalance()
                    .transform("compact_task",
                            TypeInformation.of(CompactionCommitEvent.class),
                            new ProcessOperator<>(new CompactFunction(conf)))
                    .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
                    .addSink(new CompactionCommitSink(conf))
                    .name("compact_commit")
                    .setParallelism(1); // compaction commit should be singleton
        } else {
            pipeline.addSink(new CleanFunction<>(conf))
                    .setParallelism(1)
                    .name("clean_commits").uid("uid_clean_commits");
        }

        env.execute(cfg.targetTableName);
    }

}
