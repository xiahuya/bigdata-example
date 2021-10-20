package cn.xhjava.hoodie.callback.service;

import cn.xhjava.hoodie.callback.domain.HoodieCallbackMsg;
import org.springframework.stereotype.Service;

/**
 * @author Xiahu
 * @create 2021-06-16
 */

public interface HoodieCallbackService {

    public void hoodieCallbackOption(HoodieCallbackMsg msg);

}
