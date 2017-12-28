package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForLoop;
import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.interfaces.standarder.IStandarder;
import com.baiyyy.didcs.service.flow.StdNodeService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 通用标准化处理节点
 *
 * @author 逄林
 */
public class CommonStdCleanNodeForLoop extends AbstractCleanNodeForLoop implements ICleanNode {
    private Logger logger = null;
    private StdNodeService stdNodeService = null;
    //Map<fieldName,Map<>>
    private Map<String,Map<String,Object>> stdConfMap = null;
    private List<String> fieldList = null;

    @Override
    public void operate(Map data) throws Exception {
        for(String field:fieldList){
            if(MapUtil.isNotBlank(data.get(field+"_stdstr"))&&MapUtil.isBlank(data.get(field+"_stdid"))){
                IStandarder standarder = (IStandarder)stdConfMap.get(field).get("class");

                String[] stdResult = standarder.doStd(MapUtil.getCasedString(data.get(field+"_stdstr")), MapUtil.getCasedString(stdConfMap.get(field).get("param")), data, MapUtil.getCasedString(stdConfMap.get(field).get("sys")));
                if(null!=stdResult){
                    getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),field+"_stdstr",data.get(field),stdResult[1], FlowConstant.CLEAN_STAGE_STD, EsConstant.LOG_TARGET_SOURCE,"","");
                    getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),field+"_stdid",data.get(field+"_stdid"),stdResult[0],FlowConstant.CLEAN_STAGE_STD, EsConstant.LOG_TARGET_SOURCE,"","");
                    data.put(field+"_stdstr",stdResult[1]);
                    data.put(field+"_stdid",stdResult[0]);
                }else{
                    data.put(field+"_stdstr","");
                    data.put(field+"_stdid","");
                }
            }
        }
    }

    @Override
    public void getConf() throws Exception {
        stdNodeService = SpringContextUtil.getBean("stdNodeService");
        //1.根据batchId获取配置方式
        Map idMap = stdNodeService.getIdConfMap(getCleanFlow().getBatchId());
        //2.获取格式化配置
        String schemaId = null;
        String taskId = null;
        if (ConfConstant.IF_TASK_CONF.equals(MapUtil.getCasedString(idMap.get("if_self_conf")))) {
            taskId = MapUtil.getCasedString(idMap.get("task_id"));
        } else {
            schemaId = MapUtil.getCasedString(idMap.get("schema_id"));
        }

        List<Map> stdConf = stdNodeService.getStdConf(schemaId,taskId);
        //3.缓存规则+格式化执行器
        stdConfMap = new HashMap<>();
        fieldList = new ArrayList<>();
        for(Map map:stdConf){
            //拼装执行程序
            Map<String,Object> innerMap = new HashMap();
            IStandarder standarder = (IStandarder)Class.forName(MapUtil.getCasedString(map.get("class_name"))).newInstance();
            //执行器初始化
            standarder.setClient(getCleanFlow().getClient());
            innerMap.put("class",standarder);
            if("1".equals(MapUtil.getCasedString(map.get("if_std_cust")))){
                innerMap.put("sys",idMap.get("sys"));
            }else{
                innerMap.put("sys","");
            }
            innerMap.put("param",map.get("param"));
            stdConfMap.put(MapUtil.getCasedString(map.get("code")),innerMap);
            //按顺序排列字段
            fieldList.add(MapUtil.getCasedString(map.get("code")));
        }
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonStdCleanNodeForLoop.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "标准化节点";
    }
}
