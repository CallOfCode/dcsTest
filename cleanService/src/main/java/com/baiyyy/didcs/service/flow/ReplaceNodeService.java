package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.ReplaceNodeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 替换相关service
 *
 * @author 逄林
 */
@Service
public class ReplaceNodeService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private ReplaceNodeMapper replaceNodeMapper;

    /**
     * 根据批次ID获取所采用的配置
     *
     * @param batchId
     * @return
     */
    public Map getSchemaConfMap(String batchId) {
        return replaceNodeMapper.selectIdInfoByBatchId(batchId);
    }

    /**
     * 获取替换配置，采用shcemaId和taskId中不为空的值对应的配置
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    public List<Map> getReplaceConf(String schemaId, String taskId) {
        return replaceNodeMapper.selectReplaceConfs(schemaId, taskId);
    }

    /**
     * 获取对应缓存代码
     * @param schemaId
     * @return
     */
    public String getCacheCode(String schemaId){
        return MapUtil.getCasedString(replaceNodeMapper.selectCacheCode(schemaId).get("cache_code"));
    }
}
