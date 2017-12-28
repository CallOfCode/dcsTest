package com.baiyyy.didcs.interfaces.checker;

/**
 * 检查接口
 *
 * @author 逄林
 */
public interface IChecker {

    /**
     * 初始化检查字段
     * @param fields
     */
    public void initParam(String fields);

    /**
     * 执行检查
     * @param schemaId
     * @param taskId
     * @param batchId
     */
    public void doCheck(String schemaId, String taskId, String batchId);

}
