package com.baiyyy.didcs.interfaces.flow;

import com.baiyyy.didcs.interfaces.node.ICleanNode;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;

/**
 * 流程接口
 * 流程是运行节点的调度程序，负责流程初始化、节点加载、流程启动、关闭等操作。
 *
 * @author 逄林
 */
public interface ICleanFlow {

    /**
     * 初始化方法
     * @param schemaId   元数据ID
     * @param taskId     任务ID
     * @param batchId    批次ID
     * @param userId     操作用户ID
     * @param maxId      数据最大ID
     * @param minId      数据最小ID
     * @param limitIds   ID指定值
     * @param handleSize 每批次处理数据数量
     * @param lockPath 资源锁路径
     */
    public void init(String schemaId, String taskId, String batchId, String userId, String maxId, String minId, String limitIds, Integer handleSize, String lockPath);

    /**
     * 流程启动
     */
    public void start();

    /**
     * 流程中止
     */
    public void stop();

    /**
     * 在流程开始前执行
     */
    public void beforeFlowExec();

    /**
     * 在流程节点运行完后执行
     */
    public void afterFlowExec();

    /**
     * 共享计数器-1
     */
    public void subLock();

    /**
     * 设置首节点
     *
     * @param cleanNode
     */
    public void setFirstCleanNode(ICleanNode cleanNode);

    /**
     * 获取首节点
     *
     * @return
     */
    public ICleanNode getFirstCleanNode();

    /**
     * 获取流程配置
     *
     * @return
     */
    public HashMap getConfMap();

    /**
     * 获取当前流程所在的循环数
     *
     * @return
     */
    public int getLoops();

    /**
     * 获取当前正在处理的数据
     *
     * @return
     */
    public HashMap getCurData();

    /**
     * 创建更新记录，调用该方法记录有哪些字段发生了变化
     *
     * @param id 数据ID
     * @param field 属性字段
     * @param newValue 属性值
     */
    public void updDate(String id, String field, Object newValue);

    /**
     * 创建更新记录并创建更新日志
     * @param id 数据ID
     * @param field 属性字段
     * @param oldValue 原值
     * @param newValue 新值
     * @param stage 所处清洗阶段
     * @param dataTarget 更新的目标
     * @param msg 更新的消息
     * @param userId 操作用户
     */
    public void updDateWithLog(String id, String field, Object oldValue, Object newValue, String stage, String dataTarget, String msg, String userId);

    /**
     * 更新标准数据
     * @param id 标注数据id
     * @param field 字段
     * @param strOldValue 原值
     * @param strNewValue 新值
     * @param idOldValue 原值
     * @param idNewValue 新值
     * @param refDataId 更新数据来源id
     */
    public void updStdData(String id, String field, Object strOldValue, Object strNewValue, Object idOldValue, Object idNewValue, String refDataId);

    /**
     * 通知更新标准数据
     * @param id 标准数据id
     * @param field 字段
     * @param strOldValue 原值
     * @param strNewValue 新值
     * @param idOldValue 原值
     * @param idNewValue 新值
     * @param refDataId 更新数据来源id
     */
    public void updStdDataWithNotify(String id, String field, Object strOldValue, Object strNewValue, Object idOldValue, Object idNewValue, String refDataId);

    /**
     * 设置日志列表
     *
     * @param logList
     */
    public void setLogList(List logList);

    /**
     * 组装节点列表
     *
     * @param nodes
     */
    public void makeChain(List<ICleanNode> nodes);

    /**
     * 获取元数据id
     *
     * @return
     */
    public String getSchemaId();

    /**
     * 获取任务ID
     *
     * @return
     */
    public String getTaskId();

    /**
     * 获取批次ID
     *
     * @return
     */
    public String getBatchId();

    /**
     * 获取用户ID
     *
     * @return
     */
    public String getUserId();

    /**
     * 获取最大ID
     *
     * @return
     */
    public String getMaxId();

    /**
     * 获取最小ID
     *
     * @return
     */
    public String getMinId();

    /**
     * 获取批次处理数量
     *
     * @return
     */
    public Integer getHandleSize();

    /**
     * 获取限定ID
     *
     * @return
     */
    public String getLimitIds();

    /**
     * 获取资源所路径
     * @return
     */
    public String getLockPath();

    /**
     * 获取停止标志
     *
     * @return
     */
    public Boolean getStopFlag();

    /**
     * 获取停止标志前刷新标志状态
     * 可用于主动刷新停止状态
     */
    public void refreshFlowStopFlag();
    /**
     * 获取日志列表
     *
     * @return
     */
    public List<HashMap> getLogList();

    /**
     * 获取logger
     * @return
     */
    public Logger getLogger();

    /**
     * 获取流程名称
     * @return
     */
    public String getFlowName();

    /**
     * 获取client
     * @return
     */
    public RestHighLevelClient getClient();

    /**
     * 获取index
     * @return
     */
    public String getEsIndex();

    /**
     * 获取标准表名
     * @return
     */
    public String getStdTableName();
}
