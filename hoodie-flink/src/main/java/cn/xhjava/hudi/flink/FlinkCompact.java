package cn.xhjava.hudi.flink;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.compact.*;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;

import com.beust.jcommander.JCommander;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Xiahu
 * @create 2021-06-28
 */
public class FlinkCompact {
    protected static final Logger LOG = LoggerFactory.getLogger(FlinkCompact.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkCompactionConfig cfg = new FlinkCompactionConfig();
        JCommander cmd = new JCommander(cfg, null, args);
        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }

        Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

        // create metaClient
        HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

        // get the table name
        conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

        // set table schema
        CompactionUtil.setAvroSchema(conf, metaClient);

        HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf, null);
        HoodieFlinkTable<?> table = writeClient.getHoodieTable();

        // rolls back inflight compaction first
        // condition: the schedule compaction is in INFLIGHT state for max delta seconds.
        CompactionUtil.rollbackCompaction(table, conf);

        // judge whether have operation
        // to compute the compaction instant time and do compaction.
        String compactionInstantTime = CompactionUtil.getCompactionInstantTime(metaClient);
        boolean scheduled = writeClient.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
        if (!scheduled) {
            // do nothing.
            LOG.info("No compaction plan for this job ");
            return;
        }

        table.getMetaClient().reloadActiveTimeline();
        // generate compaction plan
        // should support configurable commit metadata
        HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
                table.getMetaClient(), compactionInstantTime);

        if (compactionPlan == null || (compactionPlan.getOperations() == null)
                || (compactionPlan.getOperations().isEmpty())) {
            // No compaction plan, do nothing and return.
            LOG.info("No compaction plan for this job and instant " + compactionInstantTime);
            return;
        }

        HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
        HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
        if (!pendingCompactionTimeline.containsInstant(instant)) {
            // this means that the compaction plan was written to auxiliary path(.tmp)
            // but not the meta path(.hoodie), this usually happens when the job crush
            // exceptionally.

            // clean the compaction plan in auxiliary path and cancels the compaction.

            LOG.warn("The compaction plan was fetched through the auxiliary path(.tmp) but not the meta path(.hoodie).\n"
                    + "Clean the compaction plan in auxiliary path and cancels the compaction");
            CompactionUtil.cleanInstant(table.getMetaClient(), instant);
            return;
        }

        // get compactionParallelism.
        int compactionParallelism = Math.min(conf.getInteger(FlinkOptions.COMPACTION_TASKS), compactionPlan.getOperations().size());

        env.addSource(new CompactionPlanSourceFunction(table, instant, compactionPlan, compactionInstantTime))
                .name("compaction_source")
                .uid("uid_compaction_source")
                .rebalance()
                .transform("compact_task",
                        TypeInformation.of(CompactionCommitEvent.class),
                        new ProcessOperator<>(new CompactFunction(conf)))
                .setParallelism(compactionParallelism)
                .addSink(new CompactionCommitSink(conf))
                .name("clean_commits")
                .uid("uid_clean_commits")
                .setParallelism(1);
        env.execute("flink_hudi_compaction");
    }
}
