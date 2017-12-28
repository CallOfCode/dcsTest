package com.baiyyy.didcs.module.node;

import com.baiyyy.didcs.abstracts.node.AbstractCleanNodeForLoop;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 通用校验节点
 *
 * @author 逄林
 */
public class CommonValidateCleanNodeForLoop extends AbstractCleanNodeForLoop implements ICleanNode {
    private Logger logger = null;
    @Override
    public void operate(Map data) throws Exception {
        //TODO 校验逻辑
    }

    @Override
    public void getConf() throws Exception {

    }

    @Override
    public Logger getLogger() {
        if(null==logger){
            logger = LoggerFactory.getLogger(CommonValidateCleanNodeForLoop.class);
        }
        return logger;
    }

    @Override
    public String getNodeName() {
        return "校验节点";
    }
}
