package cn.xhjava.server.service;


import java.util.concurrent.Future;

/**
 * @author Xiahu
 * @create 2021-07-27
 */
public interface ElasticSearchJdbcRequestService {
//    Future<String> selectDataBySql(String sql);
    String selectDataBySql(String sql);
}
