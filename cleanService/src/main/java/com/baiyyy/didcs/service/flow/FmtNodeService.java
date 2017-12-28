package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.dao.flow.FmtNodeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 格式化节点service
 *
 * @author 逄林
 */
@Service
public class FmtNodeService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private FmtNodeMapper fmtNodeMapper;

    /**
     * 根据批次ID获取所采用的配置
     *
     * @param batchId
     * @return
     */
    public Map getSchemaConfMap(String batchId) {
        return fmtNodeMapper.selectIdInfoByBatchId(batchId);
    }

    /**
     * 获取格式化配置，采用shcemaId和taskId中不为空的值对应的配置
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    public List<Map> getFmtConf(String schemaId, String taskId) {
        return fmtNodeMapper.selectFmtConfs(schemaId, taskId);
    }

    /**
     * 获取特殊字符
     * @param ruleId
     * @return
     */
    public List<Map> selectSpecialChars(String ruleId){
        return fmtNodeMapper.selectSpcsByRuleId(ruleId);
    }

}
