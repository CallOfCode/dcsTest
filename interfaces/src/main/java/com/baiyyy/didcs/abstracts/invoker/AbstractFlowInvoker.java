package com.baiyyy.didcs.abstracts.invoker;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.invoker.IFlowInvoker;
import com.baiyyy.didcs.interfaces.node.ICleanNode;

import java.util.List;

/**
 * 流程调用程序抽象类
 *
 * @author 逄林
 */
public abstract class AbstractFlowInvoker implements IFlowInvoker {
    private ICleanFlow cleanFlow = null;
    private List<ICleanNode> cleanNodeList = null;

    /**
     * 在实现类中组装Flow
     */
    @Override
    public abstract void makeFlow();

    /**
     * 在实现类中组装NodeList
     */
    @Override
    public abstract void makeNodes();

    @Override
    public void doInvoke() {
        ICleanFlow flow = getCleanFlow();
        if (null != flow) {
            flow.makeChain(getCleanNodeList());
            flow.start();
        }
    }

    @Override
    public ICleanFlow getCleanFlow() {
        return this.cleanFlow;
    }

    @Override
    public List<ICleanNode> getCleanNodeList() {
        return this.cleanNodeList;
    }

    @Override
    public void setCleanFlow(ICleanFlow cleanFlow) {
        this.cleanFlow = cleanFlow;
    }

    @Override
    public void setCleanNodeList(List<ICleanNode> cleanNodeList) {
        this.cleanNodeList = cleanNodeList;
    }
}
