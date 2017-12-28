package com.baiyyy.didcs.abstracts.flow;

import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.utils.CloneUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

/**
 * 应用于对数据进行遍历处理的流程抽象类
 * 对数据进行遍历处理的流程可以继承此类
 *
 * @author 逄林
 */
public abstract class AbstractCleanFlowForLoop implements ICleanFlow {
    private String schemaId = null;
    private String taskId = null;
    private String batchId = null;
    private String userId = null;
    private String maxId = null;
    private String minId = null;
    private String limitIds = null;
    private Integer handleSize = 200;
    private Integer loops = 0;
    private Boolean stopFlag = false;
    private String lockPath = null;

    private ICleanNode firstCleanNode = null;
    private HashMap confMap = new HashMap();
    private Map<String, HashMap> updMap = new HashMap<>();
    private List<HashMap> updList = new ArrayList<>();
    private List<HashMap> logList = new ArrayList<>();

    private HashMap curData = null;

    @Override
    public void init(String schemaId, String taskId, String batchId, String userId, String maxId, String minId, String limitIds, Integer handleSize, String lockPath) {
        this.schemaId = schemaId;
        this.taskId = taskId;
        this.batchId = batchId;
        this.userId = userId;
        this.maxId = maxId;
        this.minId = minId;
        this.limitIds = limitIds;
        if (null != handleSize) {
            this.handleSize = handleSize;
        }
        this.lockPath = lockPath;
    }

    @Override
    public void start() {
        getLogger().info(append("tags","flow").and(append("flowName",getFlowName())).and(append("batchId",getBatchId())),"flow执行");
        initConf();
        beforeFlowExec();
        if (null != getFirstCleanNode()) {
            List<HashMap> dataList = getDataList();

            while (null != dataList && dataList.size() > 0) {
                if (getStopFlag()) {
                    break;
                }
                beforeLoopExec(dataList);
                setLoops(getLoops() + 1);
                for (HashMap data : dataList) {
                    curData = data;
                    if (null != data && !data.isEmpty()) {
                        getFirstCleanNode().beginClean(CloneUtils.clone((HashMap) data));
                    } else {
                        continue;
                    }
                }
                afterLoopExec(dataList);
                dataList = null;
                if (!getStopFlag()) {
                    dataList = getDataList();
                }
            }
        }
        afterFlowExec();
        subLock();

    }

    @Override
    public void stop() {
        this.stopFlag = true;
    }

    /**
     * 分配获取获取数据列表,列表数量最大为handleSize
     *
     * @return
     */
    public abstract List<HashMap> getDataList();

    /**
     * 在流程主要逻辑执行前执行的方法
     */
    @Override
    public abstract void beforeFlowExec();

    /**
     * 在流程主要逻辑执行完成后执行的方法
     */
    @Override
    public abstract void afterFlowExec();

    /**
     * 共享锁-1
     */
    @Override
    public abstract void subLock();

    /**
     * 每个遍历执行前执行的方法
     *
     * @param list
     */
    public abstract void beforeLoopExec(List<HashMap> list);

    /**
     * 每个遍历执行后执行的方法
     *
     * @param list
     */
    public abstract void afterLoopExec(List<HashMap> list);

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
    public int getLoops() {
        return this.loops;
    }

    @Override
    public HashMap getCurData() {
        return this.curData;
    }

    @Override
    public void updDate(String id, String field, Object newValue) {
        HashMap curData = getCurData();
        if (null != curData) {
            curData.put(field, newValue);

            if (updMap.containsKey(id)) {
                updMap.get(id).put(field, newValue);
            } else {
                HashMap updData = new HashMap();
                updData.put("id", id);
                updData.put(field, newValue);
                updList.add(updData);
                updMap.put(id, updData);
            }
        }
    }

    @Override
    public void updDateWithLog(String id, String field, Object oldValue, Object newValue, String stage, String dataTarget, String msg, String userId) {
        HashMap curData = getCurData();
        if (null != curData) {
            updDate(id, field, newValue);

            List logList = getLogList();

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
    public List<HashMap> getLogList() {
        return this.logList;
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
    public String getEsIndex() {
        return null;
    }

    @Override
    public String getStdTableName() {
        return null;
    }

    private void initConf() {
        if (null != getFirstCleanNode()) {
            getFirstCleanNode().initConf();
        }
    }

    private void setLoops(int loops) {
        this.loops = loops;
    }

    public Map<String, HashMap> getUpdMap() {
        return updMap;
    }

    public void setUpdMap(Map<String, HashMap> updMap) {
        this.updMap = updMap;
    }

    public List<HashMap> getUpdList() {
        return updList;
    }

    public void setUpdList(List<HashMap> updList) {
        this.updList = updList;
    }


}
