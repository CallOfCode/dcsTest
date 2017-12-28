package com.baiyyy.didcs.dao.flow;

import com.baiyyy.didcs.dao.BaseMapper;
import com.baiyyy.didcs.dao.dispatcher.ZooConfCacheMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

/**
 * 通用批量标准化mapper
 *
 * @author 逄林
 */
@Mapper
public interface BatchStdMapper extends BaseMapper{

    @Select("select a.*,std.table_name,s.source_code,t.code table_code,b.id batch_code,t.system_id from t_biz_schema_attr a,t_biz_batch b,t_biz_task t,t_biz_schema s,t_def_stds std " +
            "WHERE b.id=#{batchId} and b.task_id=t.id and t.schema_id=a.schema_id and a.if_std=1 and a.std_table_id=std.id and t.schema_id=s.id")
    public List<Map> getNeedStdFieldsByBatchId(@Param("batchId") String batchId);

    @SelectProvider(type = BatchStdMapper.BatchStdMapperProvider.class, method = "selectFieldStdConf")
    public List<Map> selectFieldStdConf(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    static class BatchStdMapperProvider {

        public String selectFieldStdConf(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT a.code,s.if_std_cust from t_conf_std s ")
                    .append("left join t_biz_schema_attr a ")
                    .append("on s.attr_id=a.id ")
                    .append("where %s ");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "s.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "s.task_id=#{taskId}");
            }

            return sql;
        }
    }
}
