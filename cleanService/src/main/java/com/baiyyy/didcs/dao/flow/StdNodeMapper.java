package com.baiyyy.didcs.dao.flow;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

/**
 * 标准化节点mapper
 *
 * @author 逄林
 */
@Mapper
public interface StdNodeMapper {

    /**
     * 根据batchId查询对应的配置源
     * @param batchId
     * @return
     */
    @Select("SELECT b.task_id,t.schema_id,t.if_self_conf,t.code sys from t_biz_batch b,t_biz_task t " +
            "where b.task_id=t.id and b.id=#{batchId}")
    public Map selectConfMapByBatchId(@Param("batchId") String batchId);

    /**
     * 查询标准化配置
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = StdNodeMapper.StdNodeMapperProvider.class, method = "selectStdConfs")
    public List<Map> selectStdConfs(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    static class StdNodeMapperProvider {

        public String selectStdConfs(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("select a.code,f.class_name,c.if_std_cust,c.param from t_conf_std c,t_biz_schema_attr a,t_def_function f ")
                    .append("where %s and c.attr_id=a.id and c.function_id=f.id ")
                    .append("order by c.order_num asc");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "c.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "c.task_id=#{taskId}");
            }

            return sql;
        }

    }
}
