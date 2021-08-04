package cn.xhjava.curd;


import cn.xhjava.curd.config.GulimallEsSearchConfig;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


/**
 * @author Xiahu
 * @create 2021-08-04
 * 对于ES 索引数据的增删查改操作
 */
public class ElasticSearchIndexDataCurdDemo {
    private static RestHighLevelClient restHighLevelClient = null;


    @Before
    public void init() {
        restHighLevelClient = GulimallEsSearchConfig.esRestClient();
    }


    //创建索引,并新增了一条：
    @Test
    public void add() {
        IndexRequest request = new IndexRequest("es_test").id("1");
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user_name", "黄");
        jsonMap.put("post_date", new Date());
        jsonMap.put("age", 18);
        jsonMap.put("gender", "男");
        jsonMap.put("height", 180);
        jsonMap.put("address", "北京");
        request.source(jsonMap);
        IndexResponse response = null;
        try {
            response = restHighLevelClient.index(request, GulimallEsSearchConfig.COMMON_OPTIONS);
            if (response.getResult().name().equalsIgnoreCase("created")) {
                System.out.println("创建成功！");
            } else {
                System.out.println("失败！");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //批量插入数据
    @Test
    public void bathAdd() {
        /**
         * 造点假数据
         */
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("address", "北京市昌平" + 12 + i + "号");
            map.put("gender", "男");
            map.put("user_name", "黄_" + i);
            map.put("post_date", new Date());
            map.put("age", 23 + i);
            map.put("height", 155 + i);
            list.add(map);
        }
        /**
         * 批量从插入数据
         */
        BulkRequest request = new BulkRequest();
        for (int j = 0; j < list.size(); j++) {
            Map<String, Object> item = list.get(j);
            request.add(new IndexRequest("es_user").id(String.valueOf(j)).source(item));
        }
        try {
            BulkResponse bulk = restHighLevelClient.bulk(request, GulimallEsSearchConfig.COMMON_OPTIONS);
            if (bulk.status().getStatus() == 200) {
                System.out.println("批量操作成功！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    //根据id修改数据：
    @Test
    public  void updateById() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", "TTuoTHoBOK-qS7Z1MnFq");
        map.put("address", "北京市昌平" + 10082 + "号");
        map.put("gender", "nv");
        map.put("user_name", "测试修改");
        map.put("post_date", new Date());
        map.put("age", 23);
        map.put("height", 168);

        UpdateRequest request = new UpdateRequest("es_user", "1").doc(map);

        try {
            UpdateResponse update = restHighLevelClient.update(request, GulimallEsSearchConfig.COMMON_OPTIONS);
            if (update.status().getStatus() == 200) {
                System.out.println("修改成功");
            } else {
                System.out.println("修改失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void query() {

        Integer start = 1;
        Integer limit = 0;
        String keyWord = "";
        Integer minBalance = 0;
        Integer maxBalance = 0;
        String address = "";
        String city = "";
        String firstname = "";
        String employer = "";
        //1.创建请求
        SearchRequest request = new SearchRequest();
        //这里是7.4.2不需要指定type了，8以后就没有type了
        request.indices("es_user", "user_type");
        //2、创建请求参数
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        //分页并排序(第一页是从0开始的，所以上面的start-1)
        ssb.from(start).size(limit).sort("balance", SortOrder.DESC);
        //指定返回字段
        ssb.fetchSource(new String[]{"account_number", "balance", "firstname", "lastname", "age", "gender", "address", "employer", "email", "city", "state"}, new String[]{});
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        /**
         * //QueryBuilders.matchQuery()表示模糊查询----用来做keyWord的搜索
         * //QueryBuilders.termQuery()表示精确查询
         /**
         * 精确匹配:要采用 字段.keyword 才匹配得到，直接匹配那么匹配不到。原因:
         * elasticsearch 里默认的IK分词器是会将每一个中文都进行了分词的切割，所以你直接想查一整个词，或者一整句话是无返回结果的
         *
         */
        //精确匹配
        /*if (!StringUtils.isBlank(address)) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("address.keyword", address));
        }
        if (!StringUtils.isBlank(city)) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city.keyword", city));
        }
        if (!StringUtils.isBlank(employer)) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("employer.keyword", employer));
        }
        //模糊匹配
        if (!StringUtils.isBlank(firstname)) {
            boolQueryBuilder.filter(QueryBuilders.matchQuery("firstname", firstname));
        }
        if (!StringUtils.isBlank(keyWord)) {
            //多字段模糊匹配,这里是should是或者的意思
            boolQueryBuilder.filter(QueryBuilders.multiMatchQuery(keyWord, "lastname", "email", "employer"));
        }*/

        if (minBalance != null && maxBalance != null) {
            //范围查找(只针对数值，不能针对字符串)
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("balance").gte(minBalance).lte(maxBalance));
        }
        ssb.query(boolQueryBuilder);
        System.out.println("获取到的请求参数:" + ssb);
        request.source(ssb);
        Map<String, Object> map = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();
        SearchResponse response = null;
        RestStatus status = null;
        try {
            response = restHighLevelClient.search(request, GulimallEsSearchConfig.COMMON_OPTIONS);
            status = response.status();
            map.put("status", status);
            long totalHits = response.getHits().getTotalHits().value;
            Integer totalPage = (int) Math.ceil((double) totalHits / limit);
            map.put("currPage", start);
            map.put("pageSize ", limit);
            map.put("totalPage", totalPage);
            map.put("totalCount ", totalHits);
            SearchHit[] searchHits = response.getHits().getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String index = hit.getIndex();
                list.add(sourceAsMap);
            }
            map.put("list", list);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("失败");
        }
        System.out.println("查询成功:" + map.toString());
    }

    //查询多个id的数据
    @Test
    public void seleBathId() {
        SearchRequest request = new SearchRequest();
        request.indices("eslog");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //addIds后面是多个id
        boolQueryBuilder.filter(QueryBuilders.idsQuery().addIds("oI9GVHQBH0SEUrtlhvX7", "oY9HVHQBH0SEUrtlaPUO", "3Fz9aHQBxI7zG-AK_rLc"));
        builder.query(boolQueryBuilder);
        request.source(builder);
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        try {
            SearchResponse response = restHighLevelClient.search(request, GulimallEsSearchConfig.COMMON_OPTIONS);
            SearchHit[] searchHits = response.getHits().getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                list.add(sourceAsMap);
            }
            map.put("data", list);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Test
    public void deleteById() {
        DeleteRequest request = new DeleteRequest("es_user", "1");
        try {
            DeleteResponse update = restHighLevelClient.delete(request, GulimallEsSearchConfig.COMMON_OPTIONS);
            if (update.status().getStatus() == 200) {
                System.out.println("删除成功");
            } else {
                System.out.println("删除失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

