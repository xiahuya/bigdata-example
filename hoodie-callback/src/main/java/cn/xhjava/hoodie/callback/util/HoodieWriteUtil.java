package cn.xhjava.hoodie.callback.util;

import cn.xhjava.hoodie.callback.domain.ApplicationConf;
import org.apache.avro.Schema;
import org.apache.hudi.hive.HiveSyncConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2021-06-18
 */
public class HoodieWriteUtil {

    public Map<String, String> buildPamater() {
        Map<String, String> map = new HashMap<>();
        map.put("hoodie.datasource.compaction.async.enable", "true");
        map.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");
        map.put("hoodie.datasource.write.insert.drop.duplicates", "false");
        map.put("hoodie.datasource.write.partitionpath.field", "p_create_time");
        map.put("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload");
        return map;
    }

    public String buildSchema() {
        String[] fieldName = (String[]) Arrays.asList("id", "fk_id", " qfxh", " jdpj", " nioroa", " gwvz", " joqtf", " isdeleted", " lastupdatedttm", " rowkey").toArray();
        String[] fieldType = (String[]) Arrays.asList("varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar", " varchar").toArray();
        Schema schema = SchemaUtil.schemaParse(fieldName, fieldType, "xh.hoodieTableSink");
        return schema.toString();
    }


    public static HiveSyncConfig buildSyncConfig(String basePath, String tableName, Boolean isPartitionTable, ApplicationConf conf) {
        HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
        hiveSyncConfig.basePath = basePath;
        hiveSyncConfig.databaseName = conf.getHiveSyncDatabase();
        hiveSyncConfig.tableName = tableName;
        hiveSyncConfig.hiveUser = conf.getHiveSyncUsername();
        hiveSyncConfig.hivePass = conf.getHiveSyncPassword();
        hiveSyncConfig.jdbcUrl = conf.getHiveSyncJdbcUrl();
        List<String> partitionFiledList = Arrays.asList(conf.getGetHivePartitionField().split(","));
        hiveSyncConfig.partitionFields = partitionFiledList;
        if (isPartitionTable) {
            hiveSyncConfig.partitionValueExtractorClass = "org.apache.hudi.hive.MultiPartKeysValueExtractor";
        } else {
            hiveSyncConfig.partitionValueExtractorClass = "org.apache.hudi.hive.NonPartitionedExtractor";
        }

        return hiveSyncConfig;
    }


}
