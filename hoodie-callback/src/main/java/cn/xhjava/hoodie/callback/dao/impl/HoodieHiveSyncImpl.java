package cn.xhjava.hoodie.callback.dao.impl;

import cn.xhjava.hoodie.callback.dao.HoodieHiveSync;
import cn.xhjava.hoodie.callback.domain.ApplicationConf;
import cn.xhjava.hoodie.callback.domain.HoodieCallbackMsg;
import cn.xhjava.hoodie.callback.util.HoodieWriteUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021-06-18
 */
@Service
public class HoodieHiveSyncImpl implements HoodieHiveSync {
    @Autowired
    private ApplicationConf conf;


    @Override
    public void syncHive(HoodieCallbackMsg msg) {
        syncHivea(msg);
    }

    private void syncHivea(HoodieCallbackMsg msg) {
        HiveSyncConfig hiveSyncConfig = HoodieWriteUtil.buildSyncConfig(msg.getBasePath(), msg.getTableName(), true, conf);
        FileSystem fs = null;
        HiveConf hiveConf = new HiveConf();
        try {
            fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
    }
}
