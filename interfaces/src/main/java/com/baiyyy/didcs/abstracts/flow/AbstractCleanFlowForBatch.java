package com.baiyyy.didcs.abstracts.flow;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 应用于对数据进行批量处理（即每个Node节点独立运行，数据不在Node之间遍历）的流程抽象类
 * 对数据进行批量处理的流程可以继承此类
 *
 * @author 逄林
 */
public abstract class AbstractCleanFlowForBatch implements ICleanFlow {
    private String schemaId = null;
    private String taskId = null;
    private String batchId = null;
    private String userId = null;
    private String maxId = null;
    private String minId = null;
    private String limitIds = null;
    private Integer handleSize = 200;
    private Boolean stopFlag = false;
    private String lockPath = null;

    private ICleanNode firstCleanNode = null;
    private HashMap confMap = new HashMap();
    private List<HashMap> logList = new ArrayList<>();

    @Override
    public void init(String schemaId, String taskId, String batchId, String userId, String maxId, String minId, String limitIds, Integer handleSize, String lockPath) {
        this.schemaId = schemaId;
        this.taskId = taskId;
        this.batchId = batchId;
        this.userId = userId;
        this.maxId = maxId;
        this.minId = minId;
        this.limitIds = limitIds;
        this.lockPath = lockPath;
        if (null != handleSize) {
            this.handleSize = handleSize;
        }
    }

    @Override
    public void start() {
        if (null != getFirstCleanNode()) {
            beforeFlowExec();
            getFirstCleanNode().beginClean();
            afterFlowExec();
        }
        subLock();
    }

    @Override
    public void stop() {
        this.stopFlag = true;
    }

    /**
     * 在流程执行前执行的功能
     */
    @Override
    public abstract void beforeFlowExec();

    /**
     * 在流程执行后执行的功能
     */
    @Override
    public abstract void afterFlowExec();

    /**
     * 共享锁-1
     */
    @Override
    public abstract void subLock();

    @Override
    public void setFirstCleanNode(ICleanNode cleanNode) {
        this.firstCleanNode = cleanNode;
    }

    @Override
    public ICleanNode getFirstCleanNode() {
        return this.firstCleanNode;
    }

    @Override
    public HashMap getConfMap() {
        return this.confMap;
    }

    @Override
    @Deprecated
    public int getLoops() {
        return 0;
    }

    @Override
    @Deprecated
    public HashMap getCurData() {
        return null;
    }

    @Override
    @Deprecated
    public void updDate(String id, String field, Object newValue) {

    }

    @Override
    public void updDateWithLog(String id, String field, Object oldValue, Object newValue, String stage, String dataTarget, String msg, String userId) {
        HashMap log = new HashMap();
        log.put("batch_id",getBatchId());
        log.put("data_id", id);
        log.put("col", field.toUpperCase());
        log.put("old_val", oldValue);
        log.put("new_val", newValue);
        log.put("stage", stage);
        log.put("target", dataTarget);
        log.put("user_id", userId);
        log.put("msg", msg);
        logList.add(log);
    }

    @Override
    public void updStdData(String id, String field, Object strOldValue, Object strNewValue, Object idOldValue, Object idNewValue, String refDataId) {

    }

    @Override
    public void updStdDataWithNotify(String id, String field, Object strOldValue, Object strNewValue, Object idOldValue, Object idNewValue, String refDataId) {

    }

    @Override
    public void setLogList(List logList) {
        this.logList = logList;
    }

    @Override
    public void makeChain(List<ICleanNode> nodes) {
        ICleanNode tempNode = null;

        for (int i = 0; i < nodes.size(); i++) {
            ICleanNode node = nodes.get(i);
            node.setCleanFlow(this);

            if (null == tempNode) {
                tempNode = node;
            } else {
                tempNode.setNextCleanNode(node);
                tempNode = node;
            }

            if (i == 0) {
                this.setFirstCleanNode(node);
            }
        }
    }

    @Override
    public String getSchemaId() {
        return this.schemaId;
    }

    @Override
    public String getTaskId() {
        return this.taskId;
    }

    @Override
    public String getBatchId() {
        return this.batchId;
    }

    @Override
    public String getUserId() {
        return this.userId;
    }

    @Override
    public String getMaxId() {
        return this.maxId;
    }

    @Override
    public String getMinId() {
        return this.minId;
    }

    @Override
    public Integer getHandleSize() {
        return this.handleSize;
    }

    @Override
    public String getLimitIds() {
        return this.limitIds;
    }

    @Override
    public String getLockPath() {
        return this.lockPath;
    }

    @Override
    public Boolean getStopFlag() {
        refreshFlowStopFlag();
        return this.stopFlag;
    }

    @Override
    public void refreshFlowStopFlag() {

    }

    @Override
    public List<HashMap> getLogList() {
        return this.logList;
    }

    @Override
    public String getEsIndex() {
        return null;
    }

    @Override
    public String getStdTableName() {
        return null;
    }
}