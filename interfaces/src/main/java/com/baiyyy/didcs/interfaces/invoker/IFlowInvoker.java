package com.baiyyy.didcs.interfaces.invoker;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;

import java.util.List;

/**
 * 流程调用者接口
 * Flow将由FlowInvoker调用
 *
 * @author 逄林
 */
public interface IFlowInvoker {

    /**
     * 创建Flow
     */
    public void makeFlow();

    /**
     * 创建Nodes
     */
    public void makeNodes();

    /**
     * 发起流程调用
     */
    public void doInvoke();

    /**
     * 获取清洗流程
     *
     * @return
     */
    public ICleanFlow getCleanFlow();

    /**
     * 获取节点列表
     *
     * @return
     */
    public List<ICleanNode> getCleanNodeList();

    /**
     * 设置Flow
     *
     * @param cleanFlow
     */
    public void setCleanFlow(ICleanFlow cleanFlow);

    /**
     * 设置nodes
     *
     * @param cleanNodeList
     */
    public void setCleanNodeList(List<ICleanNode> cleanNodeList);
}
