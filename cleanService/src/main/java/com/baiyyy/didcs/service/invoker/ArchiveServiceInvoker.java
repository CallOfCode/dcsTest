package com.baiyyy.didcs.service.invoker;

import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.constant.SpringCloudConstant;
import com.baiyyy.didcs.common.es.EsCacheSchemaJsonUtil;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.es.EsIndexUtil;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.ZookeeperUtil;
import com.baiyyy.didcs.interfaces.invoker.IStageServiceInvoker;
import com.baiyyy.didcs.service.dispatcher.LockService;
import com.baiyyy.didcs.service.flow.ArchiveService;
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
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static net.logstash.logback.marker.Markers.append;

/**
 * 数据归档调用程序
 *
 * @author 逄林
 */
@Service
public class ArchiveServiceInvoker extends AbstractStageServiceInvoker implements IStageServiceInvoker {
    private Logger logger = LoggerFactory.getLogger(ArchiveServiceInvoker.class);
    @Autowired
    LockService lockService;
    @Autowired
    DiscoveryClient discoveryClient;
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    ArchiveService archiveService;

    @Override
    public String getInvokerName() {
        return "数据归档";
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public LockService getLockService() {
        return lockService;
    }

    @Override
    public List<Map<String, Object>> getHeadIds(int threads, Map<String, Object> param) {
        List<Map<String ,Object>> list = new ArrayList<>(threads);
        try{
            Map sqlMap = archiveService.getSql(MapUtil.getCasedString(param.get("batchId")));
            //1.获取最大最小值及数据总量
            Map codeMap = archiveService.selectCodeInfoByBatchId(MapUtil.getCasedString(param.get("batchId")));
            Map totalMap = archiveService.selectMaxMinTotal(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")),MapUtil.getCasedString(param.get("minId"),"0"),MapUtil.getCasedString(param.get("maxId")),sqlMap);
            Integer total = MapUtil.getCasedInteger(totalMap.get("total"));
            if(null==total || total==0){
                //如果总数为0，则将所有线程的开始结束Id均置为0
                for(int i=0;i<threads;i++){
                    Map tmpIdMap = new HashMap();
                    tmpIdMap.put("maxId","0");
                    tmpIdMap.put("minId","0");
                    list.add(i,tmpIdMap);
                }
            }else if(total<=1000){
                //如果总数小于指定值，则只使用一个线程进行处理
                Map rumMap = new HashMap();
                rumMap.put("maxId",param.get("maxId"));
                rumMap.put("minId",param.get("minId"));
                list.add(0,rumMap);
                for(int i=1;i<threads;i++){
                    Map tmpIdMap = new HashMap();
                    tmpIdMap.put("maxId","0");
                    tmpIdMap.put("minId","0");
                    list.add(i,tmpIdMap);
                }
            }else{
                int splitSize = total/threads;
                int nextMinId = MapUtil.getCasedInteger(totalMap.get("minId"));
                int maxId = MapUtil.getCasedInteger(totalMap.get("maxId"));
                for(int i=0;i<threads;i++){
                    //2.拆分id,生成多份param
                    Map tmpIdMap = null;
                    if(i==threads-1){
                        //最后一段则不使用Limit参数
                        tmpIdMap = archiveService.selectMaxMinLimit(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")),String.valueOf(nextMinId), String.valueOf(maxId), null, sqlMap);
                    }else{
                        tmpIdMap = archiveService.selectMaxMinLimit(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")),String.valueOf(nextMinId), String.valueOf(maxId), String.valueOf(splitSize), sqlMap);
                        nextMinId = MapUtil.getCasedInteger(tmpIdMap.get("maxId"))+1;
                    }
                    list.add(i,tmpIdMap);
                }
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_FLOW),LogConstant.LGS_FLOW_ERRORMSG_MIDDLE,e );
        }

        return list;
    }

    @Override
    public Integer initThreadNums() {
        List<ServiceInstance> list = discoveryClient.getInstances(SpringCloudConstant.CLEAN_SERVICE_NAME);
        if(null!=list&&list.size()>0){
            return list.size();
        }else{
            return SpringCloudConstant.DEFAULT_THREADS_NUM;
        }
    }

    @Override
    public void beforeInvoker(){
        //在调用服务前先校验并初始化es缓存
        //index名称
        String name = archiveService.selectCacheCodeByBatchId(getBatchId()).get("cache_code").toString();
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
                if(!EsIndexUtil.existsIndex(restClient,index)){
                    //如果没有则创建新索引
                    EsIndexUtil.createIndex(restClient,index, EsCacheSchemaJsonUtil.getIndexBody(name));
                    //创建version索引
                    String versionIndex = EsConstant.INDEX_PREFIX + "versions";
                    if(!EsIndexUtil.existsIndex(restClient,versionIndex)){
                        //创建新索引
                        EsIndexUtil.createIndex(restClient,versionIndex, "{}");
                        restClient.update(new UpdateRequest(versionIndex,EsConstant.DEFAULT_TYPE,UUID.randomUUID().toString()).doc("versionNum", System.currentTimeMillis(),"type",name).upsert("versionNum", System.currentTimeMillis(),"type",name));
                    }
                    logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_CREATE_FINISH);
                }
            }else{
                logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("index",name)),LogConstant.LGS_ES_SUCCMSG_INIT_BLOCK);
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES).and(append("cacheName",name)),LogConstant.LGS_ES_ERRORMSG_INDEX,e);
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
    }
}
