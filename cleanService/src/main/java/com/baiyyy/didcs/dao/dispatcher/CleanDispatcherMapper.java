package com.baiyyy.didcs.dao.dispatcher;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.Map;

/**
 * 流程调度相关方法
 *
 * @author 逄林
 */
@Mapper
public interface CleanDispatcherMapper {
    /**
     * 更新批次清洗状态
     * @param batchId
     * @param status
     */
    @Update("update t_biz_batch set if_start=#{status} where id=#{batchId}")
    public void updateBatchStatus(@Param("batchId") String batchId, @Param("status") String status);

    @Select("select b.task_id,t.schema_id from t_biz_batch b left join t_biz_task t on b.task_id=t.id where b.id=#{batchId}")
    public Map getSchemaIdAndTaskIdByBatchId(@Param("batchId") String batchId);

}
