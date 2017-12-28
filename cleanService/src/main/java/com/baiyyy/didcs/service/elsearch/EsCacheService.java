package com.baiyyy.didcs.service.elsearch;

import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.es.EsCacheSchemaJsonUtil;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.es.EsIndexUtil;
import com.baiyyy.didcs.common.util.DateTimeUtil;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.ZookeeperUtil;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.dao.es.EsAliasMapper;
import com.baiyyy.didcs.service.dispatcher.LockService;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.CloseableUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static net.logstash.logback.marker.Markers.append;

/**
 * Es缓存刷新服务
 *
 * @author 逄林
 */
@Service
public class EsCacheService {
    Logger logger = LoggerFactory.getLogger(EsCacheService.class);
    @Autowired
    private LockService lockService;
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private EsAliasMapper esAliasMapper;

    /**
     * 初始化指定的缓存
     * @param name
     * @return
     */
    public JsonResult initEsCacheByName(String name){
        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_INIT_BEGIN);
        JsonResult r = new JsonResult();
        r.setResult(true);
        //index名称
        String index = EsConstant.INDEX_PREFIX + name;
        //锁路径
        String lockPath = EsConstant.INDEX_INIT_LOCK_PREFIX + name;
        CuratorFramework lockClient = null;
        InterProcessMutex lock = null;
        RestHighLevelClient restClient = null;
        try{
            lockClient = ZookeeperUtil.getClient();
            //1.对name进行加锁
            if(null==lockClient.checkExists().forPath(lockPath)){
                lockClient.create().creatingParentsIfNeeded().forPath(lockPath);
            }
            lock = lockService.createLock(lockClient,lockPath);
            if(null!=lock){
                restClient = EsClientFactory.getDefaultRestClient();
                //2.根据name检查是否存在相应的索引
                if(EsIndexUtil.existsIndex(restClient,index)){
                    //如果有，则删除原索引
                    EsIndexUtil.deleteIndex(restClient,index);
                }
                //创建新索引
                EsIndexUtil.createIndex(restClient,index, EsCacheSchemaJsonUtil.getIndexBody(name));
                //3.根据name刷新数据
                int minId = -1;
                int limit = 10000;
                //4.获取多值字段及分隔符
                List<Map> multiFields = esAliasMapper.getStdSchemaMultiAttrByCode(name);
                String multiFieldName = null;
                Integer ifMultiFieldStd = null;

                List<Map> dataList = esAliasMapper.getStdDataWithAlias(name,minId,limit);
                BulkRequest request = null;
                BulkResponse responses = null;
                String[] nameArr = null;
                String[] nArr = null;
                String aliasNameStr = null;

                while(null!=dataList && dataList.size()>0){
                    request = new BulkRequest();
                    request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    for(Map data:dataList){
                        //处理多值字段
                        for(Map field:multiFields){
                            multiFieldName = field.get("code").toString();
                            ifMultiFieldStd = MapUtil.getCasedInteger(field.get("if_std"));
                            if(data.containsKey(multiFieldName)){
                                data.put(multiFieldName,MapUtil.getCasedString(data.get(multiFieldName)).split(field.get("sp_char").toString()));
                            }
                            //如果多值字段需要进行格式化
                            if(1==ifMultiFieldStd){
                                if(data.containsKey(multiFieldName+"_id")){
                                    data.put(multiFieldName+"_id",MapUtil.getCasedString(data.get(multiFieldName+"_id")).split(field.get("sp_char").toString()));
                                }
                            }
                        }

                        //处理别名字段
                        if(data.containsKey(EsConstant.DEFAULT_ALIAS_FIELDNAME)){
                            aliasNameStr = MapUtil.getCasedString(data.get(EsConstant.DEFAULT_ALIAS_FIELDNAME));
                            if(StringUtils.isNotBlank(aliasNameStr)){
                                nameArr = aliasNameStr.split("_,_");
                                for(String aname:nameArr){
                                    nArr = aname.split("\\$");
                                    if(StringUtils.isNotBlank(nArr[0])){
                                        if(data.containsKey("alias_"+nArr[0])){
                                            ((List)data.get("alias_"+nArr[0])).add(nArr[1]);
                                        }else{
                                            List aList = new ArrayList();
                                            aList.add(nArr[1]);
                                            data.put("alias_"+nArr[0],aList);
                                        }
                                    }
                                }
                            }
                            data.remove(EsConstant.DEFAULT_ALIAS_FIELDNAME);
                        }

                        request.add(new IndexRequest(index, EsConstant.DEFAULT_TYPE, MapUtil.getCasedString(data.get("id")))
                                .source(data));
                    }
                    minId = MapUtil.getCasedInteger(dataList.get(dataList.size()-1).get("id"));
                    responses = restClient.bulk(request);
                    if(responses.hasFailures()){
                        BulkItemResponse.Failure failure = null;
                        for (BulkItemResponse bulkItemResponse : responses) {
                            if (bulkItemResponse.isFailed()) {
                                failure = bulkItemResponse.getFailure();
                                logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",failure.getIndex())).and(append("id",failure.getId())).and(append("message",failure.getMessage())),LogConstant.LGS_ES_ERRORMSG_INDEX_BULK,failure.getCause());
                            }
                        }
                    }
                    dataList.clear();
                    dataList = esAliasMapper.getStdDataWithAlias(name,minId,limit);
                }
                //4.根据name，更新版本信息
                String versionIndex = EsConstant.INDEX_PREFIX + "versions";
                if(!EsIndexUtil.existsIndex(restClient,versionIndex)){
                    //创建新索引
                    EsIndexUtil.createIndex(restClient,versionIndex, "{}");
                }
                SearchResponse searchResponse = restClient.search(new SearchRequest(versionIndex).source(new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(QueryBuilders.matchPhraseQuery("type",name)))));
                SearchHits hits = searchResponse.getHits();
                String versionId = null;
                if(hits.getTotalHits()==1){
                    versionId = hits.getHits()[0].getId();
                }else{
                    versionId = UUID.randomUUID().toString();
                }

                restClient.update(new UpdateRequest(versionIndex,EsConstant.DEFAULT_TYPE,versionId).doc("versionNum", System.currentTimeMillis(),"type",name).upsert("versionNum", System.currentTimeMillis(),"type",name));
                logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_INIT_FINISH);
            }else{
                logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_INIT_BLOCK);
                r.setResult(false);
                r.setMsg("有进程占用刷新，稍后再试");
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("cacheName",name)),LogConstant.LGS_ES_ERRORMSG_INDEX,e);
            r.setResult(false);
            r.setMsg(name+"缓存初始化失败");
        }finally {
            if(null!=lock){
                try {
                    lock.release();
                } catch (Exception e) {
                    logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("cacheName",name)),LogConstant.LGS_ES_ERRORMSG_RELEASELOCK,e);
                }
            }
            CloseableUtils.closeQuietly(lockClient);
            if(null!=restClient){
                try {
                    restClient.close();
                } catch (IOException e) {
                    logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e);
                }
            }
        }
        return r;
    }

    /**
     * 刷新指定版本之后的数据
     * @param name
     * @return
     */
    public JsonResult refreshEsCacheByName(String name){
        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_REFRESH_BEGIN);
        JsonResult r = new JsonResult();
        r.setResult(true);
        //index名称
        String index = EsConstant.INDEX_PREFIX + name;
        //锁路径
        String lockPath = EsConstant.INDEX_INIT_LOCK_PREFIX + name;
        CuratorFramework lockClient = null;
        InterProcessMutex lock = null;
        RestHighLevelClient restClient = null;
        try{
            lockClient = ZookeeperUtil.getClient();
            //1.对name进行加锁
            if(null==lockClient.checkExists().forPath(lockPath)){
                lockClient.create().creatingParentsIfNeeded().forPath(lockPath);
            }
            lock = lockService.createLock(lockClient,lockPath);
            if(null!=lock){
                restClient = EsClientFactory.getDefaultRestClient();
                //根据name，更新版本信息
                String versionIndex = EsConstant.INDEX_PREFIX + "versions";
                if(!EsIndexUtil.existsIndex(restClient,versionIndex)){
                    //创建新索引
                    EsIndexUtil.createIndex(restClient,versionIndex, "{}");
                }
                SearchResponse searchResponse = restClient.search(new SearchRequest(versionIndex).source(new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(QueryBuilders.matchPhraseQuery("type",name)))));
                SearchHits hits = searchResponse.getHits();
                String versionId = null;
                String version = null;
                if(hits.getTotalHits()==1){
                    //获取版本
                    SearchHit hit = hits.getHits()[0];
                    versionId = hit.getId();
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    version = DateTimeUtil.getYYYYMMDDHHMMSS((Long) sourceAsMap.get("versionNum"));
                }
                if(StringUtils.isBlank(versionId)){
                    //不存在version则不进行刷新
                    r.setCode("-1");
                    r.setResult(false);
                }else{
                    //获取待刷新别名，为不存在的别名sys创建mapping
                    List<String> aliasStringList = esAliasMapper.getAliasSysByUpdTime(name,version);
                    if(null!=aliasStringList && aliasStringList.size()>0){
                        String mappingStr = EsIndexUtil.getIndexMappingInfo(restClient,index);
                        if(StringUtils.isNotBlank(mappingStr)){
                            JSONObject mapping = JSONObject.parseObject(mappingStr);
                            JSONObject property = mapping.getJSONObject(index).getJSONObject("mappings").getJSONObject("doc").getJSONObject("properties");
                            String jsonStr = "";
                            for(String alias:aliasStringList){
                                if(!property.containsKey("alias_"+alias)){
                                    if("".equals(jsonStr)){
                                        jsonStr += "\""+"alias_"+alias+"\":{\"type\":\"keyword\"}";
                                    }else{
                                        jsonStr += ",\""+"alias_"+alias+"\":{\"type\":\"keyword\"}";
                                    }
                                }
                            }
                            if(!"".equals(jsonStr)){
                                EsIndexUtil.addMappingInfo(restClient,index,jsonStr);
                            }
                        }
                    }

                    //根据name刷新数据
                    int limit = 10000;
                    //4.获取多值字段及分隔符
                    List<Map> multiFields = esAliasMapper.getStdSchemaMultiAttrByCode(name);
                    String multiFieldName = null;
                    Integer ifMultiFieldStd = null;
                    //获取待刷新数据id
                    List<String> updIds = esAliasMapper.getNeedUpdDataByUpdTime(name,version);
                    if(null!=updIds && updIds.size()>0){
                        int batch = updIds.size()%limit==0?updIds.size()/limit:(updIds.size()/limit+1);
                        List subList = null;
                        String ids = null;

                        BulkRequest request = null;
                        BulkResponse responses = null;
                        String[] nameArr = null;
                        String[] nArr = null;
                        String aliasNameStr = null;
                        List<Map> dataList = null;

                        for(int i=0;i<batch;i++){
                            if(i<(batch-1)){
                                subList = updIds.subList(i*limit,(i+1)*limit);
                            }else{
                                subList = updIds.subList(i*limit,updIds.size());
                            }
                            ids = StringUtils.join(subList,",");

                            dataList = esAliasMapper.getStdDataWithAliasByIds(name,ids);
                            if(null!=dataList && dataList.size()>0){
                                request = new BulkRequest();
                                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                                for(Map data:dataList){
                                    //处理多值字段
                                    for(Map field:multiFields){
                                        multiFieldName = field.get("code").toString();
                                        ifMultiFieldStd = MapUtil.getCasedInteger(field.get("if_std"));
                                        if(data.containsKey(multiFieldName)){
                                            data.put(multiFieldName,MapUtil.getCasedString(data.get(multiFieldName)).split(field.get("sp_char").toString()));
                                        }
                                        //如果多值字段需要进行格式化
                                        if(1==ifMultiFieldStd){
                                            if(data.containsKey(multiFieldName+"_id")){
                                                data.put(multiFieldName+"_id",MapUtil.getCasedString(data.get(multiFieldName+"_id")).split(field.get("sp_char").toString()));
                                            }
                                        }
                                    }

                                    //处理别名字段
                                    if(data.containsKey(EsConstant.DEFAULT_ALIAS_FIELDNAME)){
                                        aliasNameStr = MapUtil.getCasedString(data.get(EsConstant.DEFAULT_ALIAS_FIELDNAME));
                                        if(StringUtils.isNotBlank(aliasNameStr)){
                                            nameArr = aliasNameStr.split("_,_");
                                            for(String aname:nameArr){
                                                nArr = aname.split("\\$");
                                                if(StringUtils.isNotBlank(nArr[0])){
                                                    if(data.containsKey("alias_"+nArr[0])){
                                                        ((List)data.get("alias_"+nArr[0])).add(nArr[1]);
                                                    }else{
                                                        List aList = new ArrayList();
                                                        aList.add(nArr[1]);
                                                        data.put("alias_"+nArr[0],aList);
                                                    }
                                                }
                                            }
                                        }
                                        data.remove(EsConstant.DEFAULT_ALIAS_FIELDNAME);
                                    }

                                    request.add(new UpdateRequest(index, EsConstant.DEFAULT_TYPE, MapUtil.getCasedString(data.get("id"))).doc(data).upsert(data));
                                }
                                responses = restClient.bulk(request);
                                if(responses.hasFailures()){
                                    BulkItemResponse.Failure failure = null;
                                    for (BulkItemResponse bulkItemResponse : responses) {
                                        if (bulkItemResponse.isFailed()) {
                                            failure = bulkItemResponse.getFailure();
                                            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",failure.getIndex())).and(append("id",failure.getId())).and(append("message",failure.getMessage())),LogConstant.LGS_ES_ERRORMSG_UPDATE_BULK,failure.getCause());
                                        }
                                    }
                                }
                                dataList.clear();
                            }
                        }
                        //刷新版本号
                        restClient.update(new UpdateRequest(versionIndex,EsConstant.DEFAULT_TYPE,versionId).doc("versionNum", System.currentTimeMillis(),"type",name).upsert("versionNum", System.currentTimeMillis(),"type",name));
                    }
                }
                logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_REFRESH_FINISH);
            }else{
                logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_REFRESH_BLOCK);
                r.setResult(false);
                r.setMsg("有进程占用刷新，稍后再试");
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("cacheName",name)),LogConstant.LGS_ES_ERRORMSG_UPDATE,e);
            r.setResult(false);
            r.setMsg(name+"缓存更新失败");
        }finally {
            if(null!=lock){
                try {
                    lock.release();
                } catch (Exception e) {
                    logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("cacheName",name)),LogConstant.LGS_ES_ERRORMSG_RELEASELOCK,e);
                }
            }
            CloseableUtils.closeQuietly(lockClient);
            if(null!=restClient){
                try {
                    restClient.close();
                } catch (IOException e) {
                    logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e);
                }
            }
        }
        return r;
    }

    /**
     * 新增指定缓存的指定数据
     * 不会更新版本信息
     * @param name
     * @param ids
     * @return
     */
    public JsonResult addEsCacheById(String name,String ids){
        RestHighLevelClient restClient = EsClientFactory.getDefaultRestClient();
        JsonResult r = addEsCacheById(restClient,name,ids);
        if(null!=restClient){
            try {
                restClient.close();
            } catch (IOException e) {
                logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e);
            }
        }
        return r;
    }

    /**
     * 新增指定缓存的指定数据
     * 不会更新版本信息
     * @param restClient
     * @param name
     * @param ids
     * @return
     */
    public JsonResult addEsCacheById(RestHighLevelClient restClient,String name,String ids){
        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_REFRESH_BEGIN);
        JsonResult r = new JsonResult();
        r.setResult(true);
        //index名称
        String index = EsConstant.INDEX_PREFIX + name;
        CuratorFramework lockClient = null;

        try{
            if(!EsIndexUtil.existsIndex(restClient,index)){
                r.setResult(false);
                r.setMsg("索引不存在："+index);
                logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",index)),LogConstant.LGS_ES_ERRORMSG_MISSING);
                return r;
            }
            //4.获取多值字段及分隔符
            List<Map> multiFields = esAliasMapper.getStdSchemaMultiAttrByCode(name);
            String multiFieldName = null;
            Integer ifMultiFieldStd = null;
            //获取待刷新数据
            List<Map> dataList = esAliasMapper.getStdDataWithAliasByIds(name,ids);
            BulkRequest request = null;
            BulkResponse responses = null;
            String[] nameArr = null;
            String[] nArr = null;
            String aliasNameStr = null;

            if(null!=dataList && dataList.size()>0){
                request = new BulkRequest();
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for(Map data:dataList){
                    //处理多值字段
                    for(Map field:multiFields){
                        multiFieldName = field.get("code").toString();
                        ifMultiFieldStd = MapUtil.getCasedInteger(field.get("if_std"));
                        if(data.containsKey(multiFieldName)){
                            data.put(multiFieldName,MapUtil.getCasedString(data.get(multiFieldName)).split(field.get("sp_char").toString()));
                        }
                        //如果多值字段需要进行格式化
                        if(1==ifMultiFieldStd){
                            if(data.containsKey(multiFieldName+"_id")){
                                data.put(multiFieldName+"_id",MapUtil.getCasedString(data.get(multiFieldName+"_id")).split(field.get("sp_char").toString()));
                            }
                        }
                    }

                    //处理别名字段
                    if(data.containsKey(EsConstant.DEFAULT_ALIAS_FIELDNAME)){
                        aliasNameStr = MapUtil.getCasedString(data.get(EsConstant.DEFAULT_ALIAS_FIELDNAME));
                        if(StringUtils.isNotBlank(aliasNameStr)){
                            nameArr = aliasNameStr.split("_,_");
                            for(String aname:nameArr){
                                nArr = aname.split("\\$");
                                if(StringUtils.isNotBlank(nArr[0])){
                                    if(data.containsKey("alias_"+nArr[0])){
                                        ((List)data.get("alias_"+nArr[0])).add(nArr[1]);
                                    }else{
                                        List aList = new ArrayList();
                                        aList.add(nArr[1]);
                                        data.put("alias_"+nArr[0],aList);
                                    }
                                }
                            }
                        }
                        data.remove(EsConstant.DEFAULT_ALIAS_FIELDNAME);
                    }

                    request.add(new UpdateRequest(index, EsConstant.DEFAULT_TYPE, MapUtil.getCasedString(data.get("id"))).doc(data).upsert(data));
                }
                responses = restClient.bulk(request);
                if(responses.hasFailures()){
                    BulkItemResponse.Failure failure = null;
                    for (BulkItemResponse bulkItemResponse : responses) {
                        if (bulkItemResponse.isFailed()) {
                            failure = bulkItemResponse.getFailure();
                            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",failure.getIndex())).and(append("id",failure.getId())).and(append("message",failure.getMessage())),LogConstant.LGS_ES_ERRORMSG_UPDATE_BULK,failure.getCause());
                        }
                    }
                }
                dataList.clear();
            }
            logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_REFRESH_FINISH);
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("cacheName",name)),LogConstant.LGS_ES_ERRORMSG_UPDATE,e);
            r.setResult(false);
            r.setMsg(name+"缓存更新失败");
        }finally {
            CloseableUtils.closeQuietly(lockClient);
        }
        return r;
    }

    /**
     * 创建系统缓存
     * @param index
     * @return
     */
    public JsonResult creanteSysEsCacheByIndex(String index){
        //index名称
        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",index)),LogConstant.LGS_ES_SUCCMSG_INIT_BEGIN);
        JsonResult r = new JsonResult();
        r.setResult(true);
        RestHighLevelClient restClient = null;
        try{
            restClient = EsClientFactory.getDefaultRestClient();
            //e检查是否存在相应的索引
            if(!EsIndexUtil.existsIndex(restClient,index)){
                //如果没有，则创建索引
                String body = EsCacheSchemaJsonUtil.getSysIndexBody(index);
                if(StringUtils.isNotBlank(body)){
                    EsIndexUtil.createIndex(restClient,index, body);
                }else{
                    r.setResult(false);
                    r.setMsg("无法获取index body");
                }
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",index)),LogConstant.LGS_ES_ERRORMSG_INDEX,e);
            r.setResult(false);
            r.setMsg(index+"缓存创建失败");
        }finally {
            if(null!=restClient){
                try {
                    restClient.close();
                } catch (IOException e) {
                    logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e);
                }
            }
        }
        return r;
    }
}
