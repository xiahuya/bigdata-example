package cn.xhjava.hudi.flink.compact;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanSourceFunction;
import org.apache.hudi.table.HoodieFlinkCopyOnWriteTable;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.compact.HoodieFlinkMergeOnReadTableCompactor;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author Xiahu
 * @create 2021-06-29
 */
public class HoodieCompactHelper {
    protected static final Logger LOG = LoggerFactory.getLogger(CompactionPlanSourceFunction.class);
    private Configuration conf;
    private HoodieFlinkWriteClient writeClient;
    private HoodieTableMetaClient metaClient;

    public HoodieCompactHelper(Configuration conf) {
        this.conf = conf;
        this.writeClient = StreamerUtil.createWriteClient(conf, null);
//        this.metaClient = CompactionUtil.createMetaClient(conf);
    }

    /**
     * 生成调度计划,在 .hoodie 路径下新增:xxx.compaction.requested 文件
     */
    public void scheduleCompaction() {
        writeClient.scheduleCompaction(Option.empty());
    }

    public void scheduleCompaction(String instanceTime) {
        writeClient.scheduleCompactionAtInstant(instanceTime, Option.empty());
    }

    /**
     * 如果程序在进行compact 的时候发生异常情况,需要先进行回滚
     */
    public void rollbackCompaction() {
//        CompactionUtil.rollbackCompaction(writeClient.getHoodieTable(), conf, true);
    }


    /**
     * compact 方法需要Flink 环境,暂时不用Flink版本,使用Spark进行compact
     */
    private void compact() {
        HoodieCompactionPlan compactionPlan = buildHoodieCompactionPlan();
        List<CompactionPlanEvent> eventList = buildCompactionPlanEvent(compactionPlan);
        for (CompactionPlanEvent event : eventList) {
            String instantTime = event.getCompactionInstantTime();
            CompactionOperation compactionOperation = event.getOperation();
            doCompact(this.writeClient, instantTime, compactionOperation);
        }
    }


    private HoodieCompactionPlan buildHoodieCompactionPlan() {
        /*HoodieTableMetaClient metaClient = CompactionUtil.createMetaClient(conf);
        String compactionInstantTime = CompactionUtil.getCompactionInstantTime(metaClient, true);
        HoodieCompactionPlan compactionPlan = null;
        HoodieFlinkTable<?> table = writeClient.getHoodieTable();
        try {
            compactionPlan = CompactionUtils.getCompactionPlan(table.getMetaClient(), compactionInstantTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return compactionPlan;*/
        return null;
    }

    /**
     * Mark instant as compaction inflight
     *
     * @param compactionPlan
     * @return
     */
    private List<CompactionPlanEvent> buildCompactionPlanEvent(HoodieCompactionPlan compactionPlan) {
        List<CompactionPlanEvent> result = new LinkedList<>();

        // Mark instant as compaction inflight
        this.writeClient.getHoodieTable().getActiveTimeline().transitionCompactionRequestedToInflight(getHoodieInstant());
        this.writeClient.getHoodieTable().getMetaClient().reloadActiveTimeline();

        List<CompactionOperation> operations = compactionPlan.getOperations().stream()
                .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
        LOG.info("CompactionPlanFunction compacting " + operations + " files");
        for (CompactionOperation operation : operations) {
            ((LinkedList<CompactionPlanEvent>) result).addLast(new CompactionPlanEvent(getCompactionInstantTime(), operation));
        }

        return result;
    }

    private HoodieInstant getHoodieInstant() {
        return HoodieTimeline.getCompactionRequestedInstant(getCompactionInstantTime());
    }

    private String getCompactionInstantTime() {
//        return CompactionUtil.getCompactionInstantTime(metaClient, true);
        return null;
    }


    private void doCompact(HoodieFlinkWriteClient writeClient, String instantTime, CompactionOperation compactionOperation) {
        HoodieFlinkMergeOnReadTableCompactor compactor = new HoodieFlinkMergeOnReadTableCompactor();
        try {
            compactor.compact(
                    new HoodieFlinkCopyOnWriteTable<>(
                            writeClient.getConfig(),
                            writeClient.getEngineContext(),
                            writeClient.getHoodieTable().getMetaClient()),
                    writeClient.getHoodieTable().getMetaClient(),
                    writeClient.getConfig(),
                    compactionOperation,
                    instantTime);
        } catch (IOException e) {
            LOG.error("Hoodie Flink Compact Error {}", e.getMessage());
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public HoodieFlinkWriteClient getWriteClient() {
        return writeClient;
    }

    public HoodieTableMetaClient getMetaClient() {
        return metaClient;
    }
}
