package cn.xhjava.hoodie.callback.dao;

import cn.xhjava.hoodie.callback.domain.HoodieCallbackMsg;
import org.springframework.stereotype.Service;

/**
 * @author Xiahu
 * @create 2021-06-17
 */

public interface HoodieCompaction {
    public void compact(HoodieCallbackMsg msg);
}
