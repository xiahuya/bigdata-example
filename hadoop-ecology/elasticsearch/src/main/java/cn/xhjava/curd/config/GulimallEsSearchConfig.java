package cn.xhjava.curd.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author Xiahu
 * @create 2021-08-04
 */
public class GulimallEsSearchConfig {
    public static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        COMMON_OPTIONS = builder.build();
    }

    /**
     * 方式一：无账号密码连接方式
     * new HttpHost("localhost", 9200, "http")));
     * //集群配置法
     * new HttpHost("localhost", 9201, "http")));
     **/
    public static RestHighLevelClient esRestClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        //集群配置法
                        new HttpHost("192.168.0.114", 9213, "http")));
        return client;
    }

    /**
     * 方式二 使用账号密码连接
     *
     * @return
     */
    public RestHighLevelClient esRestClientByPassword() {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("21.145.229.153", 9200, "http"),
                new HttpHost("21.145.229.253", 9200, "http"),
                new HttpHost("21.145.229.353", 9200, "http"));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "1qaz!QAZ"));
        builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient restClient = new RestHighLevelClient(builder);
        return restClient;
    }
}
