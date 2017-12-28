package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.StdNodeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 标准化节点相关service
 *
 * @author 逄林
 */
@Service
public class StdNodeService {
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private StdNodeMapper stdNodeMapper;


    /**
     * 根据批次ID获取所采用的配置
     *
     * @param batchId
     * @return
     */
    public Map getIdConfMap(String batchId) {
        return stdNodeMapper.selectConfMapByBatchId(batchId);
    }

    /**
     * 获取格式化配置，采用shcemaId和taskId中不为空的值对应的配置
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    public List<Map> getStdConf(String schemaId, String taskId) {
        return stdNodeMapper.selectStdConfs(schemaId, taskId);
    }
}
