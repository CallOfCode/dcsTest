package com.baiyyy.didcs.dao.flow;

import com.baiyyy.didcs.dao.BaseMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

/**
 * 格式化节点mapper
 *
 * @author 逄林
 */
@Mapper
public interface FmtNodeMapper extends BaseMapper{

    /**
     * 查询格式化配置
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = FmtNodeMapper.FmtNodeMapperProvider.class, method = "selectFmtConfs")
    public List<Map> selectFmtConfs(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    /**
     * 查询特殊字符
     * @param ruleId
     * @return
     */
    @Select("select * from t_conf_spc where rule_id=#{ruleId} order by order_num asc")
    public List<Map> selectSpcsByRuleId(@Param("ruleId") String ruleId);

    static class FmtNodeMapperProvider {

        public String selectFmtConfs(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("select a.code,f.class_name,c.spc_rule_id,a.if_std from t_conf_fmt c,t_biz_schema_attr a,t_def_function f ")
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
