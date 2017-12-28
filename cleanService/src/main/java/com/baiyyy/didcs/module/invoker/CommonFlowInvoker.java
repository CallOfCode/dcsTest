package com.baiyyy.didcs.module.invoker;

import com.baiyyy.didcs.abstracts.invoker.AbstractFlowInvoker;
import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.invoker.IFlowInvoker;
import com.baiyyy.didcs.interfaces.node.ICleanNode;

import java.util.List;

/**
 * 通用格式化流程调用器
 *
 * @author 逄林
 */
public class CommonFlowInvoker extends AbstractFlowInvoker implements IFlowInvoker {
    public CommonFlowInvoker(ICleanFlow cleanFlow, List<ICleanNode> cleanNodeList) {
        setCleanFlow(cleanFlow);
        setCleanNodeList(cleanNodeList);
    }

    @Override
    public void makeFlow() {

    }

    @Override
    public void makeNodes() {

    }
}
