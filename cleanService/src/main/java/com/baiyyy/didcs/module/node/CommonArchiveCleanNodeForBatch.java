package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForBatch;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.util.DateTimeUtil;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.service.elsearch.EsCacheService;
import com.baiyyy.didcs.service.elsearch.EsLogService;
import com.baiyyy.didcs.service.flow.ArchiveService;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

public class CommonArchiveCleanNodeForBatch extends AbstractCleanNodeForBatch implements ICleanNode {
    private Logger logger = null;
    private ArchiveService archiveService;
    private EsLogService esLogService;
    private EsCacheService esCacheService;

    @Override
    public void operate() throws Exception {
        archiveService = SpringContextUtil.getBean("archiveService");
        esLogService = SpringContextUtil.getBean("esLogService");
        esCacheService = SpringContextUtil.getBean("esCacheService");
        RestHighLevelClient restClient = EsClientFactory.getDefaultRestClient();
        //1.获取total
        Map sqlMap = archiveService.getSql(getCleanFlow().getBatchId());
        //1.获取最大最小值及数据总量
        Map codeMap = archiveService.selectCodeInfoByBatchId(MapUtil.getCasedString(getCleanFlow().getBatchId()));
        Map cacheCodeMap = archiveService.selectCacheCodeByBatchId(getCleanFlow().getBatchId());
        Map totalMap = archiveService.selectMaxMinTotal(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")),MapUtil.getCasedString(getCleanFlow().getMinId(),"0"),MapUtil.getCasedString(getCleanFlow().getMaxId()),sqlMap);
        int total = MapUtil.getCasedInteger(totalMap.get("total"));
        //2.分页处理
        int batchSize = 1000;
        String min = getCleanFlow().getMinId();
        String max = getCleanFlow().getMaxId();
        int batch = total%batchSize==0?total/batchSize:(total/batchSize+1);
        String cacheName = cacheCodeMap.get("cache_code").toString();

        //拼接字段
        Map fieldMap = archiveService.getArchiveFields(getCleanFlow().getBatchId());
        String sourceTable = archiveService.getSourceTableName(getCleanFlow().getBatchId());
        String sourceFields = fieldMap.get("source").toString()+",'"+getCleanFlow().getBatchId()+"',id,'"+ DateTimeUtil.getNowYYYYMMDDHHMMSS()+"'";
        String stdTable = archiveService.getStdTableName(getCleanFlow().getBatchId());
        String stdFields = fieldMap.get("std").toString()+",source_batch_id,source_data_id,add_time";

        for(int i=0;i<batch;i++){
            List<String> ids = archiveService.selectIdsWithMaxMinLimit(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")),min,max,String.valueOf(batchSize),sqlMap);
            min = String.valueOf(Integer.valueOf(ids.get(ids.size()-1))+1);
            String idStr = StringUtils.join(ids,",");
            try{
                archiveService.archiveByIds(sourceTable,sourceFields,stdTable,stdFields,idStr);
                List logList = new ArrayList();
                for(String id:ids){
                    Map log = new HashMap();
                    log.put("batch_id",getCleanFlow().getBatchId());
                    log.put("data_id", id);
                    log.put("ref_data_id","");
                    log.put("col", "data_status");
                    log.put("old_val", FlowConstant.DATA_STATUS_VALID);
                    log.put("new_val", FlowConstant.DATA_STATUS_STO);
                    log.put("stage", "sto");
                    log.put("target", EsConstant.LOG_TARGET_SOURCE);
                    log.put("user_id", "");
                    log.put("msg", "");
                    logList.add(log);
                }
                esLogService.batchInsertLogData(getCleanFlow().getClient(),logList);
                //3.刷新缓存
                //获取标准数据id
                List stdIds = archiveService.getStdIdsBySourceIds(stdTable, getCleanFlow().getBatchId(), idStr);
                if(stdIds.size()>0){
                    //进行数据刷新
                    esCacheService.addEsCacheById(restClient,cacheName,StringUtils.join(stdIds,","));
                }
            }catch(Exception e){
                getLogger().error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_NODE),LogConstant.LGS_NODE_ERRORMSG_STO,e );
            }finally {
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

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonArchiveCleanNodeForBatch.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "批量归档节点";
    }
}
