package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForLoop;
import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.formatter.IFormatter;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.service.flow.FmtNodeService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 通用格式化处理节点
 *
 * @author 逄林
 */
public class CommonFmtCleanNodeForLoop extends AbstractCleanNodeForLoop implements ICleanNode {
    private Logger logger = null;
    private FmtNodeService fmtNodeService;
    private Map<String,List<Map>> spcMap = new HashMap<>();
    private List<Map> fmtConfList = null;

    @Override
    public void operate(Map data) throws Exception {
        String field = null;
        for(Map map:fmtConfList){
            field = MapUtil.getCasedString(map.get("code"));
            IFormatter formatter = (IFormatter)map.get("class");
            Boolean needFmt = false;
            if(ConfConstant.FIELD_STD.equals(MapUtil.getCasedString(map.get("if_std")))){
                //需要格式化的字段以stdid字段为标准化判断依据
                if(!MapUtil.isBlank(getCleanFlow().getCurData().get(field))&&MapUtil.isBlank(getCleanFlow().getCurData().get(field+"_stdid"))){
                    needFmt = true;
                }
            }else{
            //需要格式化的字段以stdstr字段为标准化判断依据
                if(!MapUtil.isBlank(getCleanFlow().getCurData().get(field))&&MapUtil.isBlank(getCleanFlow().getCurData().get(field+"_stdstr"))){
                    needFmt = true;
                }
            }
            if(needFmt){
                //判断stdstr有没有值，有值，则在stdstr字段上执行，无值在原字段上执行
                String fmtStr = null;
                if(MapUtil.isBlank(data.get(field+"_stdstr"))){
                    fmtStr = formatter.doFormat(MapUtil.getCasedString(data.get(field))).toString();
                    getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),field+"_stdstr","",fmtStr,FlowConstant.CLEAN_STAGE_FMT, EsConstant.LOG_TARGET_SOURCE,"","");
                }else{
                    fmtStr = formatter.doFormat(MapUtil.getCasedString(data.get(field+"_stdstr"))).toString();
                    //stdstr不为空时，只有当数据发生变化时才更新并记录日志
                    if(!MapUtil.getCasedString(data.get(field+"_stdstr")).equals(fmtStr)){
                        getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),field+"_stdstr",data.get(field+"_stdstr"),fmtStr,FlowConstant.CLEAN_STAGE_FMT, EsConstant.LOG_TARGET_SOURCE,"","");
                    }
                }
                data.put(field+"_stdstr",fmtStr);
            }
        }
    }

    @Override
    public void getConf() throws Exception {
        fmtNodeService = SpringContextUtil.getBean("fmtNodeService");
        //1.根据batchId获取配置方式
        Map confMap = fmtNodeService.getSchemaConfMap(getCleanFlow().getBatchId());
        //2.获取格式化配置
        String schemaId = null;
        String taskId = null;
        if (ConfConstant.IF_TASK_CONF.equals(MapUtil.getCasedString(confMap.get("if_self_conf")))) {
            taskId = MapUtil.getCasedString(confMap.get("task_id"));
        } else {
            schemaId = MapUtil.getCasedString(confMap.get("schema_id"));
        }
        List<Map> fmtConfTmp = fmtNodeService.getFmtConf(schemaId, taskId);
        //3.缓存规则+格式化执行器
        for (Map conf : fmtConfTmp) {
            IFormatter formatter = (IFormatter)Class.forName(MapUtil.getCasedString(conf.get("class_name"))).newInstance();
            String ruleId = MapUtil.getCasedString(conf.get("spc_rule_id"));
            if(StringUtils.isNotBlank(ruleId)){
                if(!spcMap.containsKey(ruleId)){
                    spcMap.put(ruleId,fmtNodeService.selectSpecialChars(ruleId));
                }
                formatter.initRules(spcMap.get(ruleId));
            }
            conf.put("class",formatter);
        }

        fmtConfList = fmtConfTmp;
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonFmtCleanNodeForLoop.class);
        }
        return logger;
    }
    @Override
    public String getNodeName() {
        return "格式化节点";
    }

}
