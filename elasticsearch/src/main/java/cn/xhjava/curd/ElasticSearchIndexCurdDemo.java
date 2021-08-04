package cn.xhjava.curd;


import cn.xhjava.curd.config.GulimallEsSearchConfig;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * @author Xiahu
 * @create 2021-08-04
 * 对于ES 索引的增删查改操作!
 */

public class ElasticSearchIndexCurdDemo {
    private static RestHighLevelClient restHighLevelClient = null;


    @Before
    public void init() {
        restHighLevelClient = GulimallEsSearchConfig.esRestClient();
    }


    //创建索引
    @Test
    public void createIndex() throws IOException {
        String index = "es-7.13.3";
        Settings settings = Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
                .build();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index).settings(settings);
        CreateIndexResponse response = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

    }

    @Test
    public void deleteIndex() throws IOException {
        String index = "es-7.13.3";

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void isExist() throws IOException {
        String index = "es-7.13.3";
        IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(index);
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
}

