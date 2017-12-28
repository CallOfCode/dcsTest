package com.baiyyy.didcs.abstracts.node;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;

import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

/**
 * 用于对数据进行遍历处理的Node抽象类
 * 需要对数据进行遍历处理的节点，可以继承该抽象类
 *
 * @author 逄林
 */
public abstract class AbstractCleanNodeForLoop implements ICleanNode{
    private ICleanNode nextCleanNode;
    private ICleanFlow cleanFlow;

    /**
     * 节点逻辑处理方法
     * @param data 节点要处理的数据
     * @throws Exception
     */
    @Override
    public abstract void operate(Map data) throws Exception;

    @Override
    public void beginClean(Map data) {
        if(!ifSkip()){
            try{
                beforeOperate(data);
                operate(data);
                afterOperate(data);
            }catch(Exception e){
                getLogger().error(append("tags","node").and(append("nodeName",getNodeName())).and(append("batchId",getCleanFlow().getBatchId())).and(append("dataId",data.get("id"))),"node执行出错",e );
            }
        }
        doNext(getCleanFlow().getCurData());
    }


    @Override
    public Boolean ifSkip() {
        return false;
    }

    @Override
    public void initConf() {
        if(!ifSkip()){
            try{
                getConf();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        getNextConf();
    }

    /**
     * 节点配置获取方法
     * @throws Exception
     */
    @Override
    public abstract void getConf() throws Exception;

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
    public void beginClean() {}

    @Override
    @Deprecated
    public void operate() throws Exception {}

    /**
     * 节点操作执行前处理方法
     * @param data
     */
    public void beforeOperate(Map data){};

    /**
     * 节点操作执行后处理方法
     * @param data
     */
    public void afterOperate(Map data){};

    private void doNext(Map data){
        if(null!=getNextCleanNode()){
            getNextCleanNode().beginClean(data);
        }
    }

    private void getNextConf(){
        if(null!=getNextCleanNode()){
            getNextCleanNode().initConf();
        }
    }
}
