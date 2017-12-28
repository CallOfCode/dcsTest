package com.baiyyy.didcs.service.elsearch;

import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.es.EsCacheSchemaJsonUtil;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.util.MapUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

import static net.logstash.logback.marker.Markers.append;

public class TestEsService {

    public static void main(String[] args){
        RestHighLevelClient restClient = null;
        try{
            restClient = EsClientFactory.getRestClient("172.17.24.99",9200);
            String index = EsConstant.INDEX_PREFIX+"gender";
//            testIndexExist(restClient,index);
//            testIndexDel(restClient,index);
//            testCreateIndex(restClient,index);
//            testGetIndexInfo(restClient,index);
//            testPutMapping(restClient,index);
            testFilter(restClient);
//            testSubList();
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            if(null!=restClient){
                try {
                    restClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void testGet(RestHighLevelClient restClient) throws Exception{
        GetRequest getRequest = new GetRequest(
                "logstash-2017.11.29",
                "doc",
                "QFSABmABHX03P8Nd2m--");
        GetResponse getResponse = restClient.get(getRequest);
        if (getResponse.isExists()){
            String sourceAsString = getResponse.getSourceAsString();
            System.out.println(sourceAsString);
        }else{
            System.out.println("not exists");
        }
    }

    private static void testExist(RestHighLevelClient restClient) throws Exception{
        GetRequest getRequest = new GetRequest(
                "logstash-2017.11.29",
                "doc",
                "QFSABmABHX03P8Nd2m--");

        System.out.println(restClient.exists(getRequest));
    }

    private static void testIndexExist(RestHighLevelClient restClient,String index) throws Exception{
        Response response = restClient.getLowLevelClient().performRequest("HEAD",index, Collections.<String, String>emptyMap());
        System.out.println(response);
        System.out.println(response.getStatusLine().getStatusCode());
    }

    private static void testIndexDel(RestHighLevelClient restClient,String index) throws Exception{
        restClient.getLowLevelClient().performRequest("DELETE",index,Collections.<String, String>emptyMap());
    }

    private static void testCreateIndex(RestHighLevelClient restClient,String index) throws Exception{
        HttpEntity entity = new NStringEntity(EsCacheSchemaJsonUtil.getIndexBody("doctor"), ContentType.APPLICATION_JSON);
        restClient.getLowLevelClient().performRequest("PUT",EsConstant.INDEX_PREFIX+"doctor",Collections.emptyMap(),entity);
    }

    private static void testGetIndexInfo(RestHighLevelClient restClient,String index) throws Exception{
        Response response = restClient.getLowLevelClient().performRequest("GET",index+"/_mapping",Collections.emptyMap());
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    private static void testPutMapping(RestHighLevelClient restClient,String index) throws Exception{
        String jsonStr = "\"tweet\":{\"type\":\"keyword\"}";
        StringBuffer sb = new StringBuffer()
                .append("{")
                .append("\"properties\":{")
                .append(jsonStr)
                .append("}")
                .append("}");

        HttpEntity entity = new NStringEntity(sb.toString(), ContentType.APPLICATION_JSON);
        Response response = restClient.getLowLevelClient().performRequest("PUT",index+"/_mapping/doc",Collections.emptyMap(),entity);
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    private static void testSearch(RestHighLevelClient restClient,String index) throws Exception{
        String element = "woman";
        String aliasSys = "common";
        List<QueryBuilder> querys = new ArrayList<>();
        querys.add(QueryBuilders.matchQuery("name",element));
        querys.add(QueryBuilders.matchQuery("alias_"+aliasSys,element));

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should().addAll(querys);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(boolQueryBuilder));
        SearchRequest searchRequest = new SearchRequest(index).source(searchSourceBuilder);
        SearchResponse searchResponse = restClient.search(searchRequest);
        System.out.println(searchResponse.getHits().getTotalHits());
    }

    private static void testFilter(RestHighLevelClient restClient) throws Exception{
        String index = "clean_doctor";
        String [] fieldArr = new String[]{"hospital_stdid","section_stdid","name_stdstr","position_stdid"};
        String [] stdFieldArr = new String[]{"hospital_id","section_id","name","position_id"};
        Map data = new HashMap();
        data.put("hospital_stdid",383286);
        data.put("section_stdid",51);
        data.put("name_stdstr","孙守权");
        data.put("position_stdid",5);
        data.put("not_match_id","538002,538003");

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        for(int i=0;i<fieldArr.length;i++){
            boolBuilder.must(QueryBuilders.matchQuery(stdFieldArr[i], MapUtil.getCasedString(data.get(fieldArr[i]))));
        }
        //处理非疑似条件
        if(data.containsKey("not_match_id")){
            if(MapUtil.isNotBlank(data.get("not_match_id"))){
                String[] ids = MapUtil.getCasedString(data.get("not_match_id")).split(",");
                for(String id:ids){
                    if(StringUtils.isNotBlank(id)){
                        boolBuilder.mustNot(QueryBuilders.matchQuery("id",id));
                    }
                }
            }
        }
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(boolBuilder);
        System.out.println(new SearchSourceBuilder().query(queryBuilder).toString());
        SearchResponse searchResponse = null;
        try {
            searchResponse = restClient.search(new SearchRequest(index).source(new SearchSourceBuilder().query(queryBuilder).fetchSource(false)));
            SearchHits hits = searchResponse.getHits();
            System.out.println(hits.getTotalHits());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void testSubList(){
        List list = new ArrayList();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");
        list.add("5");
        list.add("6");
        List subList = list.subList(2,3);
        for(Object str:subList){
            System.out.println(str);
        }

    }
}
