package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForLoop;
import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.constant.ReplaceConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.matcher.IMatcher;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.interfaces.replacer.IReplacer;
import com.baiyyy.didcs.service.flow.ReplaceNodeService;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 通用数据替换节点
 *
 * @author 逄林
 */
public class CommonReplaceCleanNodeForLoop extends AbstractCleanNodeForLoop implements ICleanNode {
    private Logger logger = LoggerFactory.getLogger(CommonReplaceCleanNodeForLoop.class);
    private ReplaceNodeService replaceNodeService;
    private List<IReplacer> replacerList = null;
    private String index = null;

    @Override
    public void operate(Map data) throws Exception {
        if(MapUtil.isNotBlank(data.get("match_id")) && MapUtil.isNotBlank(data.get("data_status")) && FlowConstant.DATA_STATUS_MATCH.equals(MapUtil.getCasedString(data.get("data_status")))){
            Map stdMap = null;
            //查询标准数据
            GetRequest getRequest = new GetRequest(index,"doc",MapUtil.getCasedString(data.get("id")));
            GetResponse getResponse = getCleanFlow().getClient().get(getRequest);
            if (getResponse.isExists()) {
                stdMap = getResponse.getSourceAsMap();
            }

            //替换逻辑
            if(null!=replacerList && null!=stdMap){
                for(IReplacer replacer : replacerList){
                    List<String> list = replacer.doReplace(data,stdMap);
                    for(String field:list){
                        if(ReplaceConstant.REPLACE_NOTIFY.equals(replacer.getReplaceStrategy())){
                            getCleanFlow().updStdDataWithNotify(MapUtil.getCasedString(stdMap.get("id")),field,MapUtil.getCasedString(stdMap.get(field)), MapUtil.getCasedString(data.get(field+"_stdstr")),MapUtil.getCasedString(stdMap.get(field+"_id")), MapUtil.getCasedString(data.get(field+"_stdid")),MapUtil.getCasedString(data.get("id")));
                        }else{
                            getCleanFlow().updStdData(MapUtil.getCasedString(stdMap.get("id")),field,MapUtil.getCasedString(stdMap.get(field)), MapUtil.getCasedString(data.get(field+"_stdstr")),MapUtil.getCasedString(stdMap.get(field+"_id")), MapUtil.getCasedString(data.get(field+"_stdid")),MapUtil.getCasedString(data.get("id")));
                        }
                    }
                }
            }
        }
    }

    @Override
    public void getConf() throws Exception {
        replaceNodeService = SpringContextUtil.getBean("replaceNodeService");

        //获取匹配配置
        //1.根据batchId获取配置方式
        Map confMap = replaceNodeService.getSchemaConfMap(getCleanFlow().getBatchId());
        //2.获取格式化配置
        String schemaId = null;
        String taskId = null;
        if (ConfConstant.IF_TASK_CONF.equals(MapUtil.getCasedString(confMap.get("if_self_conf")))) {
            taskId = MapUtil.getCasedString(confMap.get("task_id"));
        } else {
            schemaId = MapUtil.getCasedString(confMap.get("schema_id"));
        }

        List<Map> confList = replaceNodeService.getReplaceConf(schemaId,taskId);
        if(null!=confList){
            replacerList = new ArrayList<>();
            for(Map conf : confList){
                IReplacer replacer = (IReplacer) Class.forName(MapUtil.getCasedString(conf.get("class_name"))).newInstance();
                replacer.initParam(MapUtil.getCasedString(conf.get("attr")),MapUtil.getCasedString(conf.get("strategy")));
                replacerList.add(replacer);
            }
        }
        //3.获取当前批次对应的标准数据index
        String cacheCode = replaceNodeService.getCacheCode(MapUtil.getCasedString(confMap.get("schema_id")));
        this.index = EsConstant.INDEX_PREFIX + cacheCode;
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonReplaceCleanNodeForLoop.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "替换节点";
    }
}
