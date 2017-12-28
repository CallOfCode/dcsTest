package com.baiyyy.didcs.interfaces.procedure;

/**
 * 作为可独立执行程序接口
 * 用于配置在流程组中
 *
 * @author 逄林
 */
public interface IProcedure {
    /**
     * 初始化参数
     *
     * @param param
     */
    public void initParam(String param);

    /**
     * 执行程序入口
     *
     * @param schemaId
     * @param taskId
     * @param batchId
     */
    public void doExec(String schemaId, String taskId, String batchId);

}
