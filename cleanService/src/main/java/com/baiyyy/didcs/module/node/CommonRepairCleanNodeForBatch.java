package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForBatch;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通用型补全节点
 *
 * @author 逄林
 */
public class CommonRepairCleanNodeForBatch extends AbstractCleanNodeForBatch implements ICleanNode {
    private Logger logger = LoggerFactory.getLogger(CommonRepairCleanNodeForBatch.class);

    @Override
    public void operate() throws Exception {
        //TODO 补全逻辑
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public String getNodeName() {
        return "补全节点";
    }
}
