package com.baiyyy.didcs.interfaces.node;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import org.slf4j.Logger;

import java.util.Map;

/**
 * 流程运行节点接口
 *
 * @author 逄林
 */
public interface ICleanNode {
    /**
     * 节点逻辑程序执行入口
     * 带参数的operate方法用于对数据进行遍历处理的场景
     *
     * @param data 节点要处理的数据
     * @throws Exception
     */
    public void operate(Map data) throws Exception;

    /**
     * 节点逻辑程序执行入口
     * 不带参数的operate方法用于对数据进行批量处理的场景，在operate方法内部查询并处理数据
     *
     * @throws Exception
     */
    public void operate() throws Exception;

    /**
     * 节点启动方法
     * 带参数的beginClean方法用于对数据进行遍历处理的场景
     *
     * @param data
     */
    public void beginClean(Map data);

    /**
     * 节点启动方法
     * 不带参数的beginClean方法用于对数据进行批量处理的场景
     */
    public void beginClean();

    /**
     * 是否跳过当前节点
     *
     * @return
     */
    public Boolean ifSkip();

    /**
     * 初始化节点配置入口
     */
    public void initConf();

    /**
     * 获取初始化配置，在initConf方法中被调用
     *
     * @throws Exception
     */
    public void getConf() throws Exception;

    /**
     * 获取当前节点所在的流程
     *
     * @return
     */
    public ICleanFlow getCleanFlow();

    /**
     * 设置当前节点所在的流程
     *
     * @param cleanFlow
     */
    public void setCleanFlow(ICleanFlow cleanFlow);

    /**
     * 获取下一节点
     *
     * @return
     */
    public ICleanNode getNextCleanNode();

    /**
     * 设置下一节点
     *
     * @param nextCleanNode
     */
    public void setNextCleanNode(ICleanNode nextCleanNode);

    /**
     * 获取logger
     * @return
     */
    public Logger getLogger();

    /**
     * 获取节点名称
     * @return
     */
    public String getNodeName();
}