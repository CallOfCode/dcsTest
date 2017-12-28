package com.baiyyy.didcs.dao.dispatcher;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * schema task相关的Dao操作
 *
 * @author 逄林
 */
@Mapper
public interface SchemaTaskMapper {

    /**
     * 根据任务Id获取对应的源数据表与标准数据表名
     * @param taskId
     * @return
     */
    @Select("select s.source_code,s.std_code,t.code from t_biz_task t,t_biz_schema s where t.schema_id=s.id and t.id=#{taskId}")
    public Map selectTableNameByTaskId(@Param("taskId") String taskId);

    /**
     * 根据任务Id获取字段属性
     * @param taskId
     * @return
     */
    @Select("select a.code,a.attr_type,a.attr_length,a.if_std from t_biz_task t,t_biz_schema s,t_biz_schema_attr a " +
            "where t.schema_id=s.id and s.id=a.schema_id and t.id=#{taskId} and a.available=1 " +
            "order by a.order_num asc")
    public List<Map> selectTableFieldsByTaskId(@Param("taskId") String taskId);

    /**
     * 获取任务对应的标准表的字段属性
     * @param tableName
     * @return
     */
    @Select("select a.* from t_def_stds s,t_def_stds_attr a where s.table_name=#{tableName} " +
            "and s.id = a.stds_id " +
            "order by a.order_num asc")
    public List<Map> selectStdTableFieldByTableName(@Param("tableName") String tableName);

    /**
     * 获取通用字段
     * @return
     */
    @Select("select a.code,a.attr_type,a.attr_length,a.if_std from t_biz_common_attr a")
    public List<Map> selectCommonTableFields();

    /**
     * 执行sql
     * @param sql
     */
    @Insert("${sql}")
    public void execSql(@Param("sql") String sql);
}
