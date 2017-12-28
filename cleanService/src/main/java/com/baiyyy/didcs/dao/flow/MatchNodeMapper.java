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
 * 匹配节点相关mapper
 *
 * @author 逄林
 */
@Mapper
public interface MatchNodeMapper extends BaseMapper {
    /**
     * 查询格式化配置
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = MatchNodeMapper.MatchNodeMapperProvider.class, method = "selectMatchConfs")
    public List<Map> selectMatchConfs(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    /**
     * 获取地理信息字段
     * @param schemaId
     * @return
     */
    @Select("SELECT a.code from t_biz_schema_attr a where a.schema_id=#{schemaId} and a.if_geo=1 limit 1")
    public String selectGeoField(@Param("schemaId") String schemaId);

    static class MatchNodeMapperProvider {

        public String selectMatchConfs(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT c.attr,c.attr_std,c.match_type,c.param,f.class_name from t_conf_match c,t_def_function f ")
                    .append("where %s and c.function_id=f.id ")
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
