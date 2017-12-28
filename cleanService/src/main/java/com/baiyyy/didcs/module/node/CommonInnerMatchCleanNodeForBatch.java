package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForBatch;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.service.elsearch.EsLogService;
import com.baiyyy.didcs.service.flow.InnerMatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 通用型批量查重节点
 *
 * @author 逄林
 */
public class CommonInnerMatchCleanNodeForBatch extends AbstractCleanNodeForBatch implements ICleanNode {
    private Logger logger = null;
    private InnerMatchService innerMatchService;
    private EsLogService esLogService;

    @Override
    public void operate() throws Exception {
        innerMatchService = SpringContextUtil.getBean("innerMatchService");
        esLogService = SpringContextUtil.getBean("esLogService");
        //1.加载配置
        List<Map> confs = innerMatchService.getInnerMatchConfMap(getCleanFlow().getBatchId());
        String tableName = innerMatchService.getSourceTableName(getCleanFlow().getBatchId());
        //2.获取重复列表
        for(Map conf:confs){
            List<Map> groups = innerMatchService.getGroups(tableName,MapUtil.getCasedString(conf.get("attr")),getCleanFlow().getBatchId());
            //3.逐个列表进行查询，处理
            if(null!=groups){
                for(Map group:groups){
                    //获取分组内的数据列表
                    List<Map> dataList = innerMatchService.getGroupDataList(tableName,MapUtil.getCasedString(conf.get("attr")),group,getCleanFlow().getBatchId());
                    Map<String,List<String>> tempMap = new HashMap();
                    if(null!=dataList && dataList.size()>1){
                        for(Map data:dataList){
                            String status = MapUtil.getCasedString(data.get("data_status"));
                            if(tempMap.containsKey(status)){
                                tempMap.get(status).add(MapUtil.getCasedString(data.get("id")));
                            }else{
                                List aList = new ArrayList();
                                aList.add(MapUtil.getCasedString(data.get("id")));
                                tempMap.put(status,aList);
                            }
                        }
                        String innerId = null;
                        //4.采用顺序为入库-匹配-疑似-重复-有效
                        String[] sortStatus = new String[]{FlowConstant.DATA_STATUS_STO,FlowConstant.DATA_STATUS_MATCH,FlowConstant.DATA_STATUS_LIKE,FlowConstant.DATA_STATUS_INNER_MATCH,FlowConstant.DATA_STATUS_VALID};
                        for(String status:sortStatus){
                            if(tempMap.containsKey(status)){
                                innerId = tempMap.get(status).get(0);
                                break;
                            }
                        }
                        //5.生成updateList
                        List<Map> updList = new ArrayList();
                        if(tempMap.containsKey(FlowConstant.DATA_STATUS_VALID)){
                            for(String id:tempMap.get(FlowConstant.DATA_STATUS_VALID)){
                                if(innerId.equals(id)){
                                    continue;
                                }else{
                                    Map updIdMap = new HashMap();
                                    updIdMap.put("id",id);
                                    updIdMap.put("match_id",innerId);
                                    updIdMap.put("data_status",FlowConstant.DATA_STATUS_INNER_MATCH);
                                    updList.add(updIdMap);
                                }
                            }
                            if(updList.size()>0){
                                //保存更新
                                innerMatchService.batchUpdateData(tableName,updList);
                                List logList = new ArrayList();
                                for(Map map:updList){
                                    Map log = new HashMap();
                                    log.put("target",EsConstant.LOG_TARGET_SOURCE);
                                    log.put("batch_id",getCleanFlow().getBatchId());
                                    log.put("data_id", map.get("id"));
                                    log.put("col", "match_id");
                                    log.put("old_val", "");
                                    log.put("new_val", map.get("match_id"));
                                    log.put("stage", FlowConstant.CLEAN_STAGE_INNERMATCH);
                                    log.put("user_id", "");
                                    log.put("msg", "");
                                    logList.add(log);
                                }
                                esLogService.batchInsertLogData(getCleanFlow().getClient(),logList);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonInnerMatchCleanNodeForBatch.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "批量格式化节点";
    }
}
