package cn.xhjava.server.controller;

import cn.xhjava.server.service.ElasticSearchJdbcRequestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
@RestController
@RequestMapping("/es")
public class ElasticSearchJdbcRequestController {
    @Autowired
    private ElasticSearchJdbcRequestService elasticSearchJdbcRequestService;

    @PostMapping("/sql")
    public String selectDataBySql(@RequestBody String sql) throws Exception {
        return elasticSearchJdbcRequestService.selectDataBySql(sql).get();
    }
}
