package com.baiyyy.didcs.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Mapper 基类
 *
 * @author 逄林
 */
@Mapper
public interface BaseMapper {

    /**
     * 执行语句
     * @param sql
     */
    @Update("${sql}")
    public void updateBySql(@Param("sql") String sql);

    /**
     * 根据batchId获取各项配置代码
     * @param batchId
     * @return
     */
    @Select("SELECT s.source_code,s.std_code,t.code task_code,b.id batch_code from t_biz_batch b,t_biz_task t,t_biz_schema s " +
            "where b.task_id=t.id and t.schema_id=s.id and b.id=#{batchId}")
    public Map selectCodeInfoByBatchId(@Param("batchId") String batchId);

    /**
     * 根据batchId获取源表表名
     * @param batchId
     * @return
     */
    @Select("SELECT CONCAT(s.source_code,'_',t.code) table_name from t_biz_batch b,t_biz_task t,t_biz_schema s " +
            "where b.task_id=t.id and t.schema_id=s.id and b.id=#{batchId}")
    public String selectSouceTableByBatchId(@Param("batchId") String batchId);

    /**
     * 根据batchId获取标准表表名
     * @param batchId
     * @return
     */
    @Select("SELECT s.std_code from t_biz_batch b,t_biz_task t,t_biz_schema s " +
            "where b.task_id=t.id and t.schema_id = s.id and b.id=#{batchId}")
    public String selectStdTableByBatchId(@Param("batchId") String batchId);
    /**
     * 根据batchId获取相应的配置 taskID和schemaID信息
     * @param batchId
     * @return
     */
    @Select("select b.id batch_id,t.if_self_conf,b.task_id,t.schema_id from t_biz_batch b,t_biz_task t " +
            "where b.task_id=t.id and b.id=#{batchId}")
    public Map selectIdInfoByBatchId(@Param("batchId") String batchId);

    /**
     * 获取缓存代码
     * @param schemaId
     * @return
     */
    @Select("select d.cache_code,d.table_name from t_biz_schema s,t_def_stds d where s.std_code=d.table_name and s.id=#{schemaId} LIMIT 1")
    public Map selectCacheCode(@Param("schemaId") String schemaId);

    /**
     * 获取属性字段
     * @param batchId
     * @return
     */
    @Select("SELECT a.* from t_biz_batch b,t_biz_task t,t_biz_schema_attr a " +
            "where b.id=#{batchId} and b.task_id=t.id and t.schema_id=a.schema_id " +
            "order by a.order_num asc")
    public List<Map> selectFieldsByBatchId(@Param("batchId") String batchId);

}
