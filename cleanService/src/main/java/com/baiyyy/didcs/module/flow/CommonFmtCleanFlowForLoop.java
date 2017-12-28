package com.baiyyy.didcs.module.flow;

import com.baiyyy.didcs.abstracts.flow.AbstractCleanFlowForLoop;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.service.dispatcher.LockService;
import com.baiyyy.didcs.service.elsearch.EsLogService;
import com.baiyyy.didcs.service.flow.FmtFlowService;
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
 * 通用遍历格式化处理流程
 *
 * @author 逄林
 */
public class CommonFmtCleanFlowForLoop extends AbstractCleanFlowForLoop implements ICleanFlow {
    private Logger logger = null;
    private FmtFlowService fmtFlowService;
    private EsLogService esLogService;
    private String loopMinId = "";
    private Integer loopSize = 10000;
    private Map codeMap = null;
    private String notNullSql = null;
    private RestHighLevelClient client = null;

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

        List<HashMap> dataList = fmtFlowService.getDataListPage(MapUtil.getCasedString(codeMap.get("source_code")), MapUtil.getCasedString(codeMap.get("task_code")),MapUtil.getCasedString(codeMap.get("batch_code")), loopMinId,getMaxId(), loopSize, notNullSql);

        if(!dataList.isEmpty()){
            loopMinId = MapUtil.getCasedString(dataList.get(dataList.size()-1).get("id"));
        }
        return dataList;
    }

    @Override
    public void beforeFlowExec() {
        //获取code
        fmtFlowService = SpringContextUtil.getBean("fmtFlowService");
        esLogService = SpringContextUtil.getBean("esLogService");
        codeMap = fmtFlowService.getCodesByBatchId(getBatchId());
        //生成非空sql
        notNullSql = fmtFlowService.getNotNullSqlByBatchId(getBatchId());
    }

    @Override
    public void afterFlowExec() {
           if(null!=client){
               try {
                   client.close();
               } catch (IOException e) {
                   logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e );
               }
           }
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
                fmtFlowService.batchUpdateData(getBatchId(),updList);
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

        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_FLOW),LogConstant.LGS_FLOW_ERRORMSG_AFTERLOOP,e );
        }

    }

    @Override
    public void subLock() {
        //减少计数器，调用流程调度结束当前清洗流程对应的调度节点并开启
        LockService lockService = SpringContextUtil.getBean("lockService");
        lockService.subLockNum(getLockPath(), getBatchId(), getUserId());
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
        return "格式化处理流程";
    }

    @Override
    public RestHighLevelClient getClient() {
        if(null==client){
            synchronized(CommonFmtCleanFlowForLoop.class){
                if(null==client){
                    client = EsClientFactory.getDefaultRestClient();
                }
            }
        }
        return client;
    }
}
