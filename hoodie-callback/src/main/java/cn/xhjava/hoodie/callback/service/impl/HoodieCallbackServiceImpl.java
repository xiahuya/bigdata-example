package cn.xhjava.hoodie.callback.service.impl;

import cn.xhjava.hoodie.callback.dao.HoodieClean;
import cn.xhjava.hoodie.callback.dao.HoodieCompaction;
import cn.xhjava.hoodie.callback.dao.HoodieHiveSync;
import cn.xhjava.hoodie.callback.domain.HoodieCallbackMsg;
import cn.xhjava.hoodie.callback.service.HoodieCallbackService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Xiahu
 * @create 2021-06-16
 */
@Service
public class HoodieCallbackServiceImpl implements HoodieCallbackService {
    @Autowired
    private HoodieClean hoodieClean;

    @Autowired
    private HoodieCompaction hoodieCompaction;

    @Autowired
    private HoodieHiveSync hoodieHiveSync;

    @Override
    public void hoodieCallbackOption(HoodieCallbackMsg msg) {
        //hoodieClean.clean(msg);
        //hoodieCompaction.compact(msg);
        hoodieHiveSync.syncHive(msg);
    }
}
