package cn.xhjava.hoodie.callback.controller;

import cn.xhjava.hoodie.callback.domain.HoodieCallbackMsg;
import cn.xhjava.hoodie.callback.service.HoodieCallbackService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author Xiahu
 * @create 2021-06-16
 */
@RestController
@RequestMapping("/hoodie")
public class HoodieCallbackClient {

    @Autowired
    private HoodieCallbackService hoodieCallbackService;

    @PostMapping("/callback")
    public void hoodieCallbackSchudeCompact(@RequestBody String json) {
        JSONObject jsonObject = JSONObject.parseObject(json);
        HoodieCallbackMsg hoodieCallbackMsg = JSON.toJavaObject(jsonObject, HoodieCallbackMsg.class);
        hoodieCallbackService.hoodieCallbackOption(hoodieCallbackMsg);
        System.out.println("********* " + json);
    }
}
