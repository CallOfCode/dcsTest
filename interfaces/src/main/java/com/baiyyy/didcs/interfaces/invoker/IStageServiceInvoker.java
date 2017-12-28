package com.baiyyy.didcs.interfaces.invoker;

/**
 * 阶段服务调用器接口
 *
 * @author 逄林
 */
public interface IStageServiceInvoker {

    /**
     * 调用服务
     * @param schemaId 元数据id
     * @param taskId 任务id
     * @param batchId 批次id
     * @param serviceJson 服务参数json
     * @param userId 用户id
     * @return
     */
    public Boolean invokerService(String schemaId,String taskId,String batchId,String serviceJson,String userId);


}
