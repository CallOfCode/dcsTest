package com.baiyyy.didcs.module.flow;

import com.baiyyy.didcs.abstracts.flow.AbstractCleanFlowForBatch;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.es.EsClientFactory;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.service.dispatcher.LockService;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static net.logstash.logback.marker.Markers.append;

/**
 * 通用批量处理清洗流程
 * 适用于调用批量处理的节点，并且在Flow中不需要进行逻辑处理
 *
 * @author 逄林
 */
public class CommonCleanFlowForBatch extends AbstractCleanFlowForBatch implements ICleanFlow {
    private Logger logger = null;
    private RestHighLevelClient client = null;
    @Override
    public void beforeFlowExec() {

    }

    @Override
    public void afterFlowExec() {
        if(null!=client){
            try {
                client.close();
            } catch (IOException e) {
                getLogger().error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_RELEASERESTCLIENT,e );
            }
        }
    }

    @Override
    public void subLock() {
        LockService lockService = SpringContextUtil.getBean("lockService");
        lockService.subLockNum(getLockPath(), getBatchId(), getUserId());
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonCleanFlowForBatch.class);
        }
        return logger;
    }

    @Override
    public String getFlowName() {
        return "批量处理流程";
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
