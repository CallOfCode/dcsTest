package com.baiyyy.didcs.module.flow;

import com.baiyyy.didcs.abstracts.flow.AbstractCleanFlowForLoop;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.service.dispatcher.LockService;
import com.baiyyy.didcs.service.elsearch.EsCacheService;
import com.baiyyy.didcs.service.elsearch.EsLogService;
import com.baiyyy.didcs.service.flow.MatchFlowService;
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

/**
 * 通用数据匹配流程
 *
 * @author 逄林
 */
public class CommonMatchCleanFlowForLoop extends AbstractCleanFlowForLoop implements ICleanFlow {
    private Logger logger = null;
    private EsLogService esLogService;
    private EsCacheService esCacheService;
    private MatchFlowService matchFlowService;
    private String loopMinId = "";
    private Integer loopSize = 10000;
    private Map codeMap = null;
    private RestHighLevelClient client = null;
    private String esIndex = null;
    private String stdTableName = null;
    private String cacheCode = null;
    private Map stdUpdFieldMap = new HashMap();//存放更新字段统计值，用于判断是否出现重复值
    private List<Map> stdUpdList = new ArrayList<>();//存放待更新列表
    private List notifyUpdList = new ArrayList();//存放待通知列表

    @Override
    public List<HashMap> getDataList() {
        if(getLoops()==0){
            //如果是第一次执行，则需要创建当前清洗的最小值
            if(StringUtils.isNotBlank(getMinId())){
                try{
                    loopMinId = String.valueOf(Integer.valueOf(getMinId())-1);
                }catch(Exception e){
                    e.printStackTrace();
                    return null;
                }
            }else{
                loopMinId = "0";
            }
        }

        List<HashMap> dataList = matchFlowService.getDataListPage(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")), loopMinId,getMaxId(), loopSize);

        if(!dataList.isEmpty()){
            loopMinId = MapUtil.getCasedString(dataList.get(dataList.size()-1).get("id"));
        }
        return dataList;
    }

    @Override
    public void beforeFlowExec() {
        //获取code
        esLogService = SpringContextUtil.getBean("esLogService");
        matchFlowService = SpringContextUtil.getBean("matchFlowService");
        esCacheService = SpringContextUtil.getBean("esCacheService");
        codeMap = matchFlowService.getCodesByBatchId(getBatchId());
        cacheCode = matchFlowService.getCacheCode(getSchemaId());
    }

    @Override
    public void afterFlowExec() {
        RestHighLevelClient client = getClient();
        if(null!=client){
            try {
                client.close();
            } catch (IOException e) {
                logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e );
            }
        }
    }

    @Override
    public void subLock() {
        //减少计数器，调用流程调度结束当前清洗流程对应的调度节点并开启
        LockService lockService = SpringContextUtil.getBean("lockService");
        lockService.subLockNum(getLockPath(), getBatchId(), getUserId());
    }

    @Override
    public void beforeLoopExec(List<HashMap> list) {

    }

    @Override
    public void afterLoopExec(List<HashMap> list) {
        try{
            //循环保存数据
            List updList = getUpdList();
            if(null!=updList&&updList.size()>0){
                //保存更新数据
                matchFlowService.batchUpdateData(getBatchId(),updList);
                //清空updList updMap
                updList.clear();
                Map updMap = getUpdMap();
                updMap.clear();
                setUpdList(new ArrayList<>());
                setUpdMap(new HashMap<>());
            }

            //记录日志
            List logList = getLogList();
            if(null!=logList&&logList.size()>0){
                esLogService.batchInsertLogData(getClient(),logList);
                logList.clear();
                setLogList(new ArrayList());
            }

            if(stdUpdList.size()>0){
                //处理标准数据更新
                matchFlowService.batchUpdateStdData(stdUpdFieldMap,stdUpdList,notifyUpdList,getBatchId(),stdTableName);
                stdUpdList.clear();
                //刷新缓存
                esCacheService.refreshEsCacheByName(cacheCode);
            }

            if(notifyUpdList.size()>0){
                //处理标准数据通知更新
                matchFlowService.batchInsertStdNotifyData(notifyUpdList);
                notifyUpdList.clear();
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_FLOW),LogConstant.LGS_FLOW_ERRORMSG_AFTERLOOP,e );
        }
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonFmtCleanFlowForLoop.class);
        }
        return logger;
    }

    @Override
    public String getFlowName() {
        return "数据匹配流程";
    }

    @Override
    public RestHighLevelClient getClient() {
        if(null==client){
            synchronized(CommonMatchCleanFlowForLoop.class){
                if(null==client){
                    client = EsClientFactory.getDefaultRestClient();
                }
            }
        }
        return client;
    }

    @Override
    public void updStdData(String id, String field, Object strOldValue, Object strNewValue, Object idOldValue, Object idNewValue, String refDataId) {
        Map tMap = new HashMap();
        tMap.put("id",id);
        tMap.put("field",field);
        tMap.put("strOldValue",strOldValue);
        tMap.put("strNewValue",strNewValue);
        tMap.put("idOldValue",idOldValue);
        tMap.put("idNewValue",idNewValue);
        tMap.put("refDataId",refDataId);
        stdUpdList.add(tMap);
        String tag = id+field;
        if(stdUpdFieldMap.containsKey(tag)){
            stdUpdFieldMap.put(tag,((Integer)stdUpdFieldMap.get(tag))+1);
        }else{
            stdUpdFieldMap.put(tag,1);
        }

        //记录更新日志
        List logList = getLogList();
        if(MapUtil.isNotBlank(strNewValue)){
            HashMap textLog = new HashMap();
            textLog.put("batch_id",getBatchId());
            textLog.put("data_id", id);
            textLog.put("ref_data_id",refDataId);
            textLog.put("col", field.toUpperCase());
            textLog.put("old_val", strOldValue);
            textLog.put("new_val", strNewValue);
            textLog.put("stage", "replace");
            textLog.put("target", EsConstant.LOG_TARGET_STD);
            textLog.put("user_id", "");
            textLog.put("msg", "");
            logList.add(textLog);
        }
        if(MapUtil.isNotBlank(idNewValue)){
            HashMap idLog = new HashMap();
            idLog.put("batch_id",getBatchId());
            idLog.put("data_id", id);
            idLog.put("ref_data_id",refDataId);
            idLog.put("col", field.toUpperCase());
            idLog.put("old_val", idOldValue);
            idLog.put("new_val", idNewValue);
            idLog.put("stage", "replace");
            idLog.put("target", EsConstant.LOG_TARGET_STD);
            idLog.put("user_id", "");
            idLog.put("msg", "");
            logList.add(idLog);
        }
    }

    @Override
    public void updStdDataWithNotify(String id, String field, Object strOldValue, Object strNewValue, Object idOldValue, Object idNewValue, String refDataId) {
        HashMap ntMap = new HashMap();
        ntMap.put("batch_id",getBatchId());
        ntMap.put("table_name",getStdTableName());
        ntMap.put("data_id", id);
        ntMap.put("attr_name", field.toLowerCase());
        ntMap.put("old_strval", strOldValue);
        ntMap.put("new_strval", strNewValue);
        ntMap.put("old_idval", idOldValue);
        ntMap.put("new_idval", idNewValue);
        ntMap.put("ref_data_id",refDataId);
        notifyUpdList.add(ntMap);
    }

    @Override
    public String getEsIndex() {
        if(null==esIndex){
            synchronized(CommonMatchCleanFlowForLoop.class){
                if(null==esIndex){
                    esIndex = EsConstant.INDEX_PREFIX + matchFlowService.getCacheCode(getSchemaId());
                }
            }
        }
        return esIndex;
    }

    @Override
    public String getStdTableName() {
        if(null==stdTableName){
            synchronized(CommonMatchCleanFlowForLoop.class){
                if(null==stdTableName){
                    stdTableName = matchFlowService.getStdTableName(getSchemaId());
                }
            }
        }
        return stdTableName;
    }
}
