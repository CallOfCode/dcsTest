package com.baiyyy.didcs.dao.flow;

import com.baiyyy.didcs.dao.BaseMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

/**
 * 替换节点mapper
 *
 * @author 逄林
 */
@Mapper
public interface ReplaceNodeMapper extends BaseMapper {
    /**
     * 查询格式化配置
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = ReplaceNodeMapper.ReplaceNodeMapperProvider.class, method = "selectReplaceConfs")
    public List<Map> selectReplaceConfs(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    static class ReplaceNodeMapperProvider {

        public String selectReplaceConfs(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT c.attr_prefix_name attr,c.strategy,f.class_name from t_conf_replace c,t_def_function f ")
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
