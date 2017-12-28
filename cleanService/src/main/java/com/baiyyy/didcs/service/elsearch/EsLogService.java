package com.baiyyy.didcs.service.elsearch;

import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

/**
 * Es日志记录service
 *
 * @author 逄林
 */
@Service
public class EsLogService {
    Logger logger = LoggerFactory.getLogger(EsLogService.class);
    /**
     * 批量插入日志数据
     * @param client
     * @param logList
     */
    public void batchInsertLogData(RestHighLevelClient client, List<Map> logList){
        if(null!=logList&&logList.size()>0){
            BulkRequest request = null;
            int batchSize = 10000;
            int b = logList.size()%batchSize==0?(logList.size()/batchSize):(logList.size()/batchSize+1);
            int min = 0;
            int max = 0;
            List<Map> lt = null;
            long date = System.currentTimeMillis();
            for(int i=0;i<b;i++){
                min = i*batchSize;
                max = (i+1)*batchSize;
                if(max>=logList.size()){
                    max = logList.size();
                }
                lt = logList.subList(min,max);
                //插入到ES
                request = new BulkRequest();
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for(Map data:lt){
                    data.put("@timestamp",date);
                    if(data.containsKey("target")&&EsConstant.LOG_TARGET_SOURCE.equals(data.get("target").toString())){
                        request.add(new IndexRequest(EsConstant.INDEX_PREFIX_CLEAN_LOG+EsConstant.LOG_TARGET_SOURCE, EsConstant.DEFAULT_TYPE, null)
                                .source(data));
                    }else if(data.containsKey("target")&&EsConstant.LOG_TARGET_STD.equals(data.get("target").toString())){
                        request.add(new IndexRequest(EsConstant.INDEX_PREFIX_CLEAN_LOG+EsConstant.LOG_TARGET_STD, EsConstant.DEFAULT_TYPE, null)
                                .source(data));
                    }
                }
                try {
                    client.bulk(request);
                } catch (IOException e) {
                    logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_LOG),LogConstant.LGS_LOG_ERRMSG_LOG,e);
                }
            }
        }
    }

}
