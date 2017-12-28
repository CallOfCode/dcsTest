package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.InnerMatchMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class InnerMatchService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private InnerMatchMapper innerMatchMapper;

    /**
     * 获取查重配置
     * @param batchId
     * @return
     */
    public List<Map> getInnerMatchConfMap(String batchId){
        Map idMap = innerMatchMapper.selectIdInfoByBatchId(batchId);
        String schemaId = MapUtil.getCasedString(idMap.get("schema_id"));
        String taskId = MapUtil.getCasedString(idMap.get("task_id"));
        String ifTaskConf = MapUtil.getCasedString(idMap.get("if_self_conf"));
        if(ConfConstant.IF_TASK_CONF.equals(ifTaskConf)){
            schemaId = null;
        }
        List<Map> fieldMap = innerMatchMapper.selectInnerMatchConf(schemaId,taskId);
        return fieldMap;
    }

    /**
     * 获取表名
     * @param batchId
     * @return
     */
    public String getSourceTableName(String batchId){
        return innerMatchMapper.selectSouceTableByBatchId(batchId);
    }

    /**
     * 根据配置的属性字段获取分组
     * @param field
     * @return
     */
    public List<Map> getGroups(String tableName,String field,String batchId){
        List list = null;
        if(StringUtils.isNotBlank(field)){
            StringBuffer notNullSql = new StringBuffer();
            String[] fieldArr = field.split(",");
            for(String f:fieldArr){
                notNullSql.append("and " + f + " is not null ");
            }
            list = innerMatchMapper.selectMatchGroups(tableName,batchId,field,notNullSql.toString());
        }
        return list;
    }

    /**
     * 获取分组内的数据
     * @param tableName
     * @param fields
     * @param group
     * @param batchId
     * @return
     */
    public List<Map> getGroupDataList(String tableName, String fields, Map group,String batchId){
        List retList = null;
        if(StringUtils.isNoneBlank(tableName,fields,batchId)&&null!=group){
            retList = innerMatchMapper.selectGroupDataList(tableName, fields, group,batchId);
        }
        return retList;
    }

    /**
     * 批量更新数据
     * @param tableName
     * @param dataList
     */
    public void batchUpdateData(String tableName, List dataList){
        //循环保存数据
        if(null!=dataList&&dataList.size()>0){
            innerMatchMapper.batchUpdateDataList(tableName,dataList);
        }
    }
}
