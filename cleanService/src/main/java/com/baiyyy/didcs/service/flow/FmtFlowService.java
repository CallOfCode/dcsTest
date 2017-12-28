package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.FmtFlowMapper;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static net.logstash.logback.marker.Markers.append;

/**
 * 格式化流程service
 *
 * @author 逄林
 */
@Service
public class FmtFlowService {
    Logger logger = LoggerFactory.getLogger(FmtFlowService.class);
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    FmtFlowMapper fmtFlowMapper;

    /**
     * 查询拼sql参数
     *
     * @param batchId
     * @return
     */
    public Map getCodesByBatchId(String batchId) {
        return fmtFlowMapper.selectCodeInfoByBatchId(batchId);
    }

    /**
     * 查询分批数据
     *
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param pageSize
     * @param notNullSql
     * @return
     */
    public List<HashMap> getDataListPage(String sourceCode, String taskCode, String batchNum, String minId, String maxId, Integer pageSize, String notNullSql) {
        return fmtFlowMapper.selectFmtDataListPage(sourceCode, taskCode, batchNum, minId, maxId, pageSize, notNullSql);
    }

    /**
     * 获取用于查询待格式化数据的sql
     *
     * @param batchId
     * @return
     */
    public String getNotNullSqlByBatchId(String batchId) {
        List<Map> fields = fmtFlowMapper.selectFieldsByBatchId(batchId);
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (Map fieldMap : fields) {
            String field = MapUtil.getCasedString(fieldMap.get("code"));
            String ifStd = MapUtil.getCasedString(fieldMap.get("if_std"));

            if (i > 0) {
                sb.append(" or");
            }
            sb.append(" (").append(field).append(" is not null");
            if ("1".equals(ifStd)) {
                sb.append(" and ").append(field).append("_stdid is null)");
            } else {
                sb.append(" and (").append(field).append("_stdstr is null or LENGTH(").append(field).append("_stdstr)<1))");
            }
            i++;
        }

        String sql = sb.toString();
        if (StringUtils.isNotBlank(sql)) {
            sql = " AND (" + sql + ")";
        }

        return sql;
    }

    /**
     * 批量更新数据
     * @param batchId
     * @param dataList
     */
    public void batchUpdateData(String batchId, List dataList){
        //循环保存数据
        if(null!=dataList&&dataList.size()>0){
            Map codeMap = getCodesByBatchId(batchId);
            String table = codeMap.get("source_code")+"_"+codeMap.get("task_code");

            int batchSize = 200;
            int b = dataList.size()%batchSize==0?(dataList.size()/batchSize):(dataList.size()/batchSize+1);
            int min = 0;
            int max = 0;
            List lt = null;
            for(int i=0;i<b;i++){
                min = i*batchSize;
                max = (i+1)*batchSize;
                if(max>=dataList.size()){
                    max = dataList.size();
                }
                lt = dataList.subList(min,max);
                //更新到数据库
                fmtFlowMapper.batchUpdateDataList(table,lt);
            }
        }
    }

}
