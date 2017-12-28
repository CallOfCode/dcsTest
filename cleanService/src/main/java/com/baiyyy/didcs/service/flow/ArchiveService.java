package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.constant.ArchiveConstant;
import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.ArchiveMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 归档相关service
 *
 * @author 逄林
 */
@Service
public class ArchiveService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private ArchiveMapper archiveMapper;

    /**
     * 查询编码
     * @param batchId
     * @return
     */
    public Map selectCodeInfoByBatchId(String batchId){
        return archiveMapper.selectCodeInfoByBatchId(batchId);
    }

    /**
     * 获取标准表及缓存代码
     * @param batchId
     * @return
     */
    public Map selectCacheCodeByBatchId(String batchId){
        return archiveMapper.selectCacheCode(batchId);
    }

    /**
     * 获取最小最大及汇总
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param sqlMap
     * @return
     */
    public Map selectMaxMinTotal(String sourceCode, String taskCode, String batchNum, String minId, String maxId, Map sqlMap){
        return archiveMapper.selectMaxMinTotal(sourceCode,taskCode,batchNum,minId,maxId,sqlMap);
    }

    /**
     * 获取待区间的最大最小值
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param limit
     * @param sqlMap
     * @return
     */
    public Map selectMaxMinLimit(String sourceCode, String taskCode, String batchNum, String minId, String maxId, String limit, Map sqlMap){
        return archiveMapper.selectMaxMinLimit(sourceCode,taskCode,batchNum,minId,maxId,limit,sqlMap);
    }

    /**
     * 获取待区间内的所有ID
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param limit
     * @param sqlMap
     * @return
     */
    public List<String> selectIdsWithMaxMinLimit(String sourceCode, String taskCode, String batchNum, String minId, String maxId, String limit, Map sqlMap){
        return archiveMapper.selectIdsWithMaxMinLimit(sourceCode,taskCode,batchNum,minId,maxId,limit,sqlMap);
    }

    /**
     * 获取归档配置
     * @param batchId
     * @return
     */
    public List<Map> getArchiveConf(String batchId){
        Map idMap = archiveMapper.selectIdInfoByBatchId(batchId);
        String schemaId = MapUtil.getCasedString(idMap.get("schema_id"));
        String taskId = MapUtil.getCasedString(idMap.get("task_id"));
        String ifTaskConf = MapUtil.getCasedString(idMap.get("if_self_conf"));
        if(ConfConstant.IF_TASK_CONF.equals(ifTaskConf)){
            schemaId = null;
        }
        return archiveMapper.getArchiveConfByBatchId(schemaId,taskId);
    }

    /**
     * 获取源表名称
     * @param batchId
     * @return
     */
    public String getSourceTableName(String batchId){
        return archiveMapper.selectSouceTableByBatchId(batchId);
    }

    /**
     * 获取标准表名
     * @param batchId
     * @return
     */
    public String getStdTableName(String batchId){
        return archiveMapper.selectStdTableByBatchId(batchId);
    }

    /**
     * 获取属性字段
     * @param batchId
     * @return
     */
    public Map getArchiveFields(String batchId){
       Map<String,String> retMap = new HashMap<>();
        List<Map> fields = archiveMapper.selectFieldsByBatchId(batchId);
        String sourceField = "";
        String stdField = "";
        for(Map field:fields){
           if("1".equals(MapUtil.getCasedString(field.get("if_std")))){
               if("".equals(sourceField)){
                   sourceField += field.get("code")+"_stdstr"+","+field.get("code")+"_stdid";
                   stdField += field.get("code")+","+field.get("code")+"_id";
               }else{
                   sourceField += ","+field.get("code")+"_stdstr"+","+field.get("code")+"_stdid";
                   stdField += ","+field.get("code")+","+field.get("code")+"_id";
               }
           }else{
               if("".equals(sourceField)){
                   sourceField += field.get("code")+"_stdstr";
                   stdField += field.get("code");
               }else{
                   sourceField += ","+field.get("code")+"_stdstr";
                   stdField += ","+field.get("code");
               }
           }
       }
       retMap.put("source",sourceField);
        retMap.put("std",stdField);
        return retMap;
    }

    /**
     * 获取拼接sql
     * @param batchId
     * @return
     */
    public Map getSql(String batchId){
        Map sqlMap = new HashMap();
        List<Map> confList = getArchiveConf(batchId);
        for(Map conf:confList){
            if(ArchiveConstant.CONF_TYPE_NOT_NULL.equals(MapUtil.getCasedString(conf.get("conf_type")))){
                String[] fields = MapUtil.getCasedString(conf.get("attr")).split(",");
                StringBuffer sb = new StringBuffer();
                for(String field:fields){
                    sb.append(" and t1.").append(field).append(" is not null");
                }
                sqlMap.put(ArchiveConstant.CONF_TYPE_NOT_NULL,sb.toString());
            }else if(ArchiveConstant.CONF_TYPE_NOT_REPEAT.equals(MapUtil.getCasedString(conf.get("conf_type")))){
                String[] fields = MapUtil.getCasedString(conf.get("attr")).split(",");
                String[] stdFields = MapUtil.getCasedString(conf.get("attr_std")).split(",");
                StringBuffer sb = new StringBuffer();
                if(fields.length==stdFields.length){
                    for(int i=0;i<fields.length;i++){
                        if(i>0){
                            sb.append(" and");
                        }
                        sb.append(" t1.").append(fields[i]).append("=").append("d.").append(stdFields[i]);
                    }
                    sqlMap.put(ArchiveConstant.CONF_TYPE_NOT_REPEAT,sb.toString());
                }
            }
        }
        return  sqlMap;
    }

    /**
     * 根据id归档数据
     * @param sourceTable
     * @param sourceFields
     * @param stdTable
     * @param stdFields
     * @param ids
     * @throws Exception
     */
    @Transactional(propagation = Propagation.REQUIRED,isolation = Isolation.DEFAULT,rollbackFor = Exception.class)
    public void archiveByIds(String sourceTable,String sourceFields, String stdTable,String stdFields, String ids) throws Exception{
        try{
            //1.导入std表
            archiveMapper.insertStdTableFromSource(sourceTable,sourceFields,stdTable,stdFields,ids);
            //2.更新数据
            archiveMapper.updateSourceTableStatus(sourceTable,ids);
        }catch(Exception e){
            throw e;
        }
    }

    /**
     * 根据源数据id获取对应的标准数据ID
     * @param stdTable
     * @param batchId
     * @param ids
     * @return
     */
    public List<String> getStdIdsBySourceIds(String stdTable,String batchId,String ids){
        return archiveMapper.getStdIdsBySourceIds(stdTable,batchId,ids);
    }

}
