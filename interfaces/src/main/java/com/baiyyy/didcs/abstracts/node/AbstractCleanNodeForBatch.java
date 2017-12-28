package com.baiyyy.didcs.abstracts.node;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import net.logstash.logback.marker.Markers;

import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

/**
 * 用于对数据进行批量处理的Node抽象类
 * 需要对数据进行批量处理的节点，可以继承该抽象类
 *
 * @author 逄林
 */
public abstract class AbstractCleanNodeForBatch implements ICleanNode {
    private ICleanNode nextCleanNode;
    private ICleanFlow cleanFlow;

    /**
     * 节点操作执行前处理方法
     */
    public void beforeOperate() {
    }

    /**
     * 节点逻辑处理
     *
     * @throws Exception
     */
    @Override
    public abstract void operate() throws Exception;

    /**
     * 节点操作执行后处理方法
     */
    public void afterOperate() {
    }

    @Override
    public void beginClean() {
        getLogger().info(append("tags","node").and(append("nodeName",getNodeName())).and(append("batchId",getCleanFlow().getBatchId())),"node执行");
        if (!ifSkip()) {
            try {
                beforeOperate();
                operate();
                afterOperate();
            } catch (Exception e) {
                getLogger().error(append("tags","node").and(append("nodeName",getNodeName())).and(append("batchId",getCleanFlow().getBatchId())),"node执行出错",e );
            }
        }
        doNext();
    }

    private void doNext() {
        if (null != getNextCleanNode()) {
            getNextCleanNode().beginClean();
        }
    }

    @Override
    public Boolean ifSkip() {
        return false;
    }


    @Override
    public ICleanFlow getCleanFlow() {
        return this.cleanFlow;
    }

    @Override
    public void setCleanFlow(ICleanFlow cleanFlow) {
        this.cleanFlow = cleanFlow;
    }

    @Override
    public ICleanNode getNextCleanNode() {
        return this.nextCleanNode;
    }

    @Override
    public void setNextCleanNode(ICleanNode nextCleanNode) {
        this.nextCleanNode = nextCleanNode;
    }

    @Override
    @Deprecated
    public void operate(Map data) throws Exception {
    }

    @Override
    @Deprecated
    public void beginClean(Map data) {
    }

    @Override
    @Deprecated
    public void initConf() {

    }

    @Override
    @Deprecated
    public void getConf() throws Exception {

    }
}
