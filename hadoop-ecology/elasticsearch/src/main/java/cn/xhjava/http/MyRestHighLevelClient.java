package cn.xhjava.http;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021-07-19
 */
public class MyRestHighLevelClient {
    public static void main(String[] args) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.0.114", 9213, "cn/xhjava/http")
                )
        );
        GetIndexRequest request = new GetIndexRequest("ab_ip_feelist");
        boolean exists = false;
        try {
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (exists) {
            System.out.println("test索引库存在");
        } else {
            System.out.println("test索引库不存在");
        }
        //关闭高级客户端实例，以便它所使用的所有资源以及底层 的http客户端实例及其线程得到正确释放
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
