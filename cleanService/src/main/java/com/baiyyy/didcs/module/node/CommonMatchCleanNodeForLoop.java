package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForLoop;
import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.formatter.IFormatter;
import com.baiyyy.didcs.interfaces.matcher.IMatcher;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.service.flow.MatchNodeService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 通用数据匹配节点
 *
 * @author 逄林
 */
public class CommonMatchCleanNodeForLoop extends AbstractCleanNodeForLoop implements ICleanNode {
    private Logger logger = null;
    List<IMatcher> matcherList = null;
    private MatchNodeService matchNodeService = null;

    @Override
    public void operate(Map data) throws Exception {
        if(null!=matcherList){
            String matchId = null;
            for(IMatcher matcher:matcherList){
                matchId = matcher.doMatch(getCleanFlow().getClient(),data);
                if(StringUtils.isNotBlank(matchId)){
                    if(FlowConstant.DATA_STATUS_MATCH.equals(matcher.getMatcherType())){
                        getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),"match_id","",matchId, FlowConstant.CLEAN_STAGE_MATCH, EsConstant.LOG_TARGET_SOURCE,"","");
                        getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),"data_status",data.get("data_status"),matcher.getMatcherType(), FlowConstant.CLEAN_STAGE_MATCH, EsConstant.LOG_TARGET_SOURCE,"","");
                        data.put("match_id",matchId);
                        data.put("data_status",matcher.getMatcherType());
                    }else if(FlowConstant.DATA_STATUS_LIKE.equals(matcher.getMatcherType())){
                        getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),"like_id",data.get("like_id"),matchId, FlowConstant.CLEAN_STAGE_MATCH, EsConstant.LOG_TARGET_SOURCE,"","");
                        getCleanFlow().updDateWithLog(MapUtil.getCasedString(data.get("id")),"data_status",data.get("data_status"),matcher.getMatcherType(), FlowConstant.CLEAN_STAGE_MATCH, EsConstant.LOG_TARGET_SOURCE,"","");
                        data.put("like_id",matchId);
                        data.put("data_status",matcher.getMatcherType());
                    }
                    break;
                }
            }
        }
    }

    @Override
    public void getConf() throws Exception {
        matchNodeService = SpringContextUtil.getBean("matchNodeService");

        //获取匹配配置
        //1.根据batchId获取配置方式
        Map confMap = matchNodeService.getSchemaConfMap(getCleanFlow().getBatchId());
        //2.获取格式化配置
        String schemaId = null;
        String taskId = null;
        if (ConfConstant.IF_TASK_CONF.equals(MapUtil.getCasedString(confMap.get("if_self_conf")))) {
            taskId = MapUtil.getCasedString(confMap.get("task_id"));
        } else {
            schemaId = MapUtil.getCasedString(confMap.get("schema_id"));
        }

        //获取地理信息字段及对应的标准表
        String cacheCode = matchNodeService.getCacheCode(MapUtil.getCasedString(confMap.get("schema_id")));
        String geoField = matchNodeService.getGeoField(MapUtil.getCasedString(confMap.get("schema_id")));

        List<Map> confList = matchNodeService.getMatchConf(schemaId,taskId);
        if(null!=confList){
            matcherList = new ArrayList<>();
            for(Map conf : confList){
                IMatcher matcher = (IMatcher)Class.forName(MapUtil.getCasedString(conf.get("class_name"))).newInstance();
                matcher.initParam(MapUtil.getCasedString(conf.get("attr")),MapUtil.getCasedString(conf.get("attr_std")), MapUtil.getCasedString(conf.get("match_type")), MapUtil.getCasedString(conf.get("param")), cacheCode, geoField);
                matcherList.add(matcher);
            }
        }
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonMatchCleanNodeForLoop.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "匹配节点";
    }
}
