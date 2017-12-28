package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForBatch;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.service.flow.BatchStdService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 通用型批量标准化节点
 *
 * @author 逄林
 */
public class CommonBatchStdCleanNodeForBatch extends AbstractCleanNodeForBatch implements ICleanNode {
    private Logger logger = null;
    private BatchStdService batchStdService;

    @Override
    public void operate() throws Exception {
        batchStdService = SpringContextUtil.getBean("batchStdService");
        List<String> sql = new ArrayList<>();
        //1.根据batchId获取schema字段
        List<Map> fields = batchStdService.getNeedStdFieldsByBatchId(getCleanFlow().getBatchId());
        Map<String,String> cusStdMap = batchStdService.getFieldStdConfMap(getCleanFlow().getBatchId());
        //2.为每个可格式化字段生成执行sql
        for (Map field : fields) {
            if(cusStdMap.containsKey(MapUtil.getCasedString(field.get("code")))){
                sql.addAll(batchStdService.getSqls(MapUtil.getCasedString(field.get("source_code")), MapUtil.getCasedString(field.get("table_code")), MapUtil.getCasedString(field.get("batch_code")), MapUtil.getCasedString(field.get("code")), MapUtil.getCasedString(field.get("table_name")), cusStdMap.get(MapUtil.getCasedString("code")), MapUtil.getCasedString(field.get("table_code")), true));
            }else{
                sql.addAll(batchStdService.getSqls(MapUtil.getCasedString(field.get("source_code")), MapUtil.getCasedString(field.get("table_code")), MapUtil.getCasedString(field.get("batch_code")), MapUtil.getCasedString(field.get("code")), MapUtil.getCasedString(field.get("table_name")), null, MapUtil.getCasedString(field.get("table_code")),false));
            }

        }
        //3.逐个执行sql
        batchStdService.execUpdSqls(sql);
    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonBatchStdCleanNodeForBatch.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "批量格式化节点";
    }
}
