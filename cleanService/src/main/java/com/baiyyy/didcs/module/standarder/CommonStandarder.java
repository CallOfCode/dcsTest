package com.baiyyy.didcs.module.standarder;

import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.interfaces.standarder.IStandarder;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

/**
 * 通用标准化执行器
 *
 * @author 逄林
 */
public class CommonStandarder implements IStandarder {
    Logger logger = LoggerFactory.getLogger(CommonStandarder.class);
    private RestHighLevelClient client;
    private String index;

    public CommonStandarder(){}

    @Override
    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String[] doStd(String element, String refElementFiledName, Map<String, Object> dataMap, String aliasSys) {
        String [] retArr = null;

        if(StringUtils.isBlank(aliasSys)){
            //默认采用common别名
            aliasSys = "common";
        }

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(StringUtils.isNotBlank(refElementFiledName)){
            //如果引用不为空的话，则必须所对应的值也必须为非空，如果某个为空，则不进行匹配
            String[] fieldArr = refElementFiledName.split(",");
            List<QueryBuilder> mustQueries = new ArrayList<>();
            boolean mustFlag = true;
            for(String field:fieldArr){
                if(dataMap.containsKey(field+"_stdstr") && MapUtil.isNotBlank(dataMap.get(field+"_stdstr"))){
                    mustQueries.add(QueryBuilders.matchQuery(field,dataMap.get(field+"_stdstr")));
                }else{
                    mustFlag = false;
                    break;
                }
            }
            if(!mustFlag){
                return null;
            }
            boolQueryBuilder.must().addAll(mustQueries);
        }

        //名称或别名匹配
        List<QueryBuilder> shoulQueries = new ArrayList<>();
        shoulQueries.add(QueryBuilders.matchQuery("name",element));
        shoulQueries.add(QueryBuilders.matchQuery("alias_"+aliasSys,element));
        boolQueryBuilder.should().addAll(shoulQueries);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(boolQueryBuilder));
        SearchRequest searchRequest = new SearchRequest(EsConstant.INDEX_PREFIX+index).source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            if(hits.getTotalHits()==1){
                //只有结果唯一时才采用
                retArr = new String[2];
                retArr[0] = hits.getHits()[0].getId();
                retArr[1] = hits.getHits()[0].getSourceAsMap().get("name").toString();
            }
        } catch (IOException e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_NODE).and(append("index",index)).and(append("element",element)),LogConstant.LGS_NODE_ERRORMSG_STD,e );
        }
        return retArr;
    }
}
