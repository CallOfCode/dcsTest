package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.MatchFlowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 匹配流程service
 *
 * @author 逄林
 */
@Service
public class MatchFlowService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private MatchFlowMapper matchFlowMapper;

    /**
     * 查询拼sql参数
     *
     * @param batchId
     * @return
     */
    public Map getCodesByBatchId(String batchId) {
        return matchFlowMapper.selectCodeInfoByBatchId(batchId);
    }

    /**
     * 获取对应缓存代码
     * @param schemaId
     * @return
     */
    public String getCacheCode(String schemaId){
        return MapUtil.getCasedString(matchFlowMapper.selectCacheCode(schemaId).get("cache_code"));
    }

    /**
     * 获取对应标准表名
     * @param schemaId
     * @return
     */
    public String getStdTableName(String schemaId){
        return MapUtil.getCasedString(matchFlowMapper.selectCacheCode(schemaId).get("table_name"));
    }

    /**
     * 查询分批数据
     *
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param pageSize
     * @return
     */
    public List<HashMap> getDataListPage(String sourceCode, String taskCode, String batchNum, String minId, String maxId, Integer pageSize) {
        return matchFlowMapper.selectNeddMatchDataListPage(sourceCode, taskCode, batchNum, minId, maxId, pageSize);
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
                matchFlowMapper.batchUpdateDataList(table,lt);
            }
        }
    }

    /**
     * 更新标准数据
     * @param stdUpdFieldMap 传入待更新字段统计
     * @param updDataList 传入待更新标准数据参数
     * @param notifyUpdList 传入通知更新列表
     * @param batchId
     * @param tableName 标准数据表名称
     */
    public void batchUpdateStdData(Map stdUpdFieldMap,List<Map> updDataList,List notifyUpdList,String batchId,String tableName){
        if(null!=updDataList){
            String tag = null;
            Map<String,Map<String,Object>> tMap = new HashMap();
            for(Map data:updDataList){
                tag = MapUtil.getCasedString(data.get("id")) + MapUtil.getCasedString(data.get("field"));
                if(stdUpdFieldMap.containsKey(tag) && ((Integer)stdUpdFieldMap.get(tag))>1){
                    //一个字段多值的，放入更新通知列表
                    HashMap ntMap = new HashMap();
                    ntMap.put("batch_id",batchId);
                    ntMap.put("table_name",tableName);
                    ntMap.put("data_id", MapUtil.getCasedString(data.get("id")));
                    ntMap.put("attr_name", MapUtil.getCasedString(data.get("field")).toLowerCase());
                    ntMap.put("old_strval", MapUtil.getCasedString(data.get("strOldValue")));
                    ntMap.put("new_strval", MapUtil.getCasedString(data.get("strNewValue")));
                    ntMap.put("old_idval", MapUtil.getCasedString(data.get("idOldValue")));
                    ntMap.put("new_idval", MapUtil.getCasedString(data.get("idNewValue")));
                    ntMap.put("ref_data_id",MapUtil.getCasedString(data.get("refDataId")));
                    notifyUpdList.add(ntMap);
                }else{
                    //按id将所有的字段组装成一个更新map
                    String id = MapUtil.getCasedString(data.get("id"));
                    if(tMap.containsKey(id)){
                        tMap.get(id).put(MapUtil.getCasedString(data.get("field")).toLowerCase(),MapUtil.getCasedString(data.get("strNewValue")));
                        tMap.get(id).put(MapUtil.getCasedString(data.get("field")).toLowerCase()+"_id",MapUtil.getCasedString(data.get("idNewValue")));
                    }else{
                        Map std = new HashMap();
                        std.put("id",MapUtil.getCasedString(data.get("id")));
                        std.put(MapUtil.getCasedString(data.get("field")).toLowerCase(),MapUtil.getCasedString(data.get("strNewValue")));
                        std.put(MapUtil.getCasedString(data.get("field")).toLowerCase()+"_id",MapUtil.getCasedString(data.get("idNewValue")));
                    }
                }
            }
            Iterator<String> it = tMap.keySet().iterator();
            List subList = new ArrayList();
            while(it.hasNext()){
                String id = it.next();
                subList.add(tMap.get(id));
                if(subList.size()>=200 || !it.hasNext()){
                    //保存到数据库
                    matchFlowMapper.batchUpdateStdDataList(tableName,subList);
                    subList.clear();
                }
            }
            tMap.clear();
            subList.clear();
        }
    }

    /**
     * 批量插入通知更新数据
     * @param notifyUpdList
     */
    public void batchInsertStdNotifyData(List notifyUpdList){
        matchFlowMapper.batchInsertNotifyDataList(notifyUpdList);
    }


}
