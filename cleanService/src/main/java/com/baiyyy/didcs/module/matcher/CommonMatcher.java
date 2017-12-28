package com.baiyyy.didcs.module.matcher;

import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.constant.MatchConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.interfaces.matcher.IMatcher;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
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
 * 通用匹配程序
 *
 * @author 逄林
 */
public class CommonMatcher implements IMatcher {
    Logger logger = LoggerFactory.getLogger(CommonMatcher.class);
    private String fields = null;
    private String stdFields = null;
    private String matchType = null;
    private String params = null;
    private String cacheCode = null;
    private String cacheIndex = null;
    private Boolean filterGeoDistance = false;
    private double minDistance = 0;
    private double maxDistance = 0;
    private String geoField = null;

    @Override
    public void initParam(String fields, String stdFields, String matchTye, String params, String cacheCode, String geoField) {
        this.fields = fields;
        this.stdFields = stdFields;
        this.matchType = matchTye;
        this.params = params;
        this.cacheCode = cacheCode;
        this.cacheIndex = EsConstant.INDEX_PREFIX + this.cacheCode;
        this.geoField = geoField;
        if(StringUtils.isNoneBlank(params,geoField)){
            //params固定形式为d:[0,5000]
            String min = params.substring(params.indexOf("[")+1,params.indexOf(","));
            String max = params.substring(params.indexOf(",")+1,params.indexOf("]"));
            if(StringUtils.isNotBlank(min)){
                minDistance = Double.valueOf(min);
            }
            if(StringUtils.isNotBlank(max)){
                maxDistance = Double.valueOf(max);
            }
            filterGeoDistance = true;
        }
    }

    @Override
    public String doMatch(RestHighLevelClient client, Object dataRow) {
        String matchId = null;
        if(!StringUtils.isNoneBlank(fields,stdFields,matchType,cacheCode)){
            return matchId;
        }
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        Map data = (Map)dataRow;
        String [] fieldArr = fields.split(",");
        String [] stdFieldArr = stdFields.split(",");
        //处理字段匹配条件
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        Boolean canMatch = true;
        for(int i=0;i<fieldArr.length;i++){
            //要求被匹配的字段必须非空，否则直接退出
            if(MapUtil.isBlank(data.get(fieldArr[i]))){
                canMatch = false;
                break;
            }
        }
        if(!canMatch){
            return matchId;
        }
        for(int i=0;i<fieldArr.length;i++){
            boolBuilder.must(QueryBuilders.matchQuery(stdFieldArr[i],data.get(fieldArr[i])));
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
        queryBuilder.filter(boolBuilder);

        if(filterGeoDistance){
            //处理地理位置匹配条件
            double lat = 0;
            double lon = 0;
            String [] latlong = MapUtil.getCasedString(data.get(geoField)).split(",");
            if(latlong.length==2){
                try{
                    lat = Double.valueOf(latlong[0]);
                    lon = Double.valueOf(latlong[1]);
                    queryBuilder.filter(QueryBuilders.geoDistanceQuery(geoField).geoDistance(GeoDistance.PLANE).point(lat,lon).distance(maxDistance, DistanceUnit.KILOMETERS));
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(new SearchRequest(cacheIndex).source(new SearchSourceBuilder().query(queryBuilder).fetchSource(false)));
        } catch (IOException e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_NODE).and(append("dataId",data.get("id"))),LogConstant.LGS_NODE_ERRORMSG_MATCH,e );
        }
        if(null!=searchResponse){
            SearchHits hits = searchResponse.getHits();
            if(MatchConstant.MATCH_TYPE_MATCH.equals(matchType)){
                if(hits.getTotalHits()==1){
                    matchId = hits.getHits()[0].getId();
                }
            }else if(MatchConstant.MATCH_TYPE_LIKE.equals(matchType)){
                List idList =  new ArrayList();
                if(hits.getTotalHits()>0){
                    for(SearchHit searchHit:hits.getHits()){
                        idList.add(searchHit.getId());
                    }
                    matchId = StringUtils.join(idList,",");
                }
            }
        }

        return matchId;
    }

    @Override
    public String getMatcherType(){
        return this.matchType;
    }
}
