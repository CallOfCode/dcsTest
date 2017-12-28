package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.MatchNodeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 匹配节点service
 *
 * @author 逄林
 */
@Service
public class MatchNodeService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private MatchNodeMapper matchNodeMapper;

    /**
     * 根据批次ID获取所采用的配置
     *
     * @param batchId
     * @return
     */
    public Map getSchemaConfMap(String batchId) {
        return matchNodeMapper.selectIdInfoByBatchId(batchId);
    }

    /**
     * 获取地理信息字段
     * @param schemaId
     * @return
     */
    public String getGeoField(String schemaId){
        return matchNodeMapper.selectGeoField(schemaId);
    }

    /**
     * 获取对应缓存代码
     * @param schemaId
     * @return
     */
    public String getCacheCode(String schemaId){
        return MapUtil.getCasedString(matchNodeMapper.selectCacheCode(schemaId).get("cache_code"));
    }

    /**
     * 获取格式化配置，采用shcemaId和taskId中不为空的值对应的配置
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    public List<Map> getMatchConf(String schemaId, String taskId) {
        return matchNodeMapper.selectMatchConfs(schemaId, taskId);
    }

}
