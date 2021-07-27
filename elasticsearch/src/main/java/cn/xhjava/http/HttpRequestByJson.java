package cn.xhjava.http;

import cn.xhjava.http.util.HttpHelper;

import java.io.IOException;

/**
 * @author Xiahu
 * @create 2021-07-16
 */
public class HttpRequestByJson {
    /**
     * java -cp   http-request-1.0-SNAPSHOT-jar-with-dependencies.jar cn.xhjava.HttpRequestByJson   http://192.168.0.114:9213/cdr/_search {"size":0,"query":{"bool":{"should":[{"range":{"executeddate":{"gte":"2019-01-01T00:00:00Z","lte":"2020-01-01T00:00:00Z","time_zone":"Asia/Shanghai"}}},{"range":{"executeddate":{"gte":"2018-01-01T00:00:00Z","lte":"2019-01-01T00:00:00Z","time_zone":"Asia/Shanghai"}}}],"minimum_should_match":1,"must":[{"term":{"isdeleted":{"value":"0"}}}]}},"aggs":{"dept_aggs":{"terms":{"field":"deptid","size":449,"order":{"_count":"desc"}},"aggs":{"cy":{"filter":{"range":{"executeddate":{"gte":"2019-01-01T00:00:00Z","lte":"2020-01-01T00:00:00Z","time_zone":"Asia/Shanghai"}}},"aggs":{"summoney":{"sum":{"field":"totalmoney"}}}},"ly":{"filter":{"range":{"executeddate":{"gte":"2018-01-01T00:00:00Z","lte":"2019-01-01T00:00:00Z","time_zone":"Asia/Shanghai"}}},"aggs":{"summoney":{"sum":{"field":"totalmoney"}}}},"joy":{"bucket_script":{"buckets_path":{"cy1":"cy.summoney","ly1":"ly.summoney"},"script":"(params.cy1-params.ly1)/params.ly1"}}}}}}
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String apiUrl = args[0];
        String json = args[1];
        HttpHelper.doPost(apiUrl, json);
    }
}
