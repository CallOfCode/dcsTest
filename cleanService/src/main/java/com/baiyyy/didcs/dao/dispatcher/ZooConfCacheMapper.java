package com.baiyyy.didcs.dao.dispatcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

/**
 * 与基于zookeeper的缓存操作相关的DAO方法
 *
 * @author 逄林
 */
@Mapper
public interface ZooConfCacheMapper {

    /**
     * 查询采用Schema配置的schemaId
     *
     * @return
     */
    @Select("select s.id from t_biz_task t,t_biz_schema s where t.schema_id=s.id and t.if_self_conf=0 and t.available=1")
    List<Map> selectAllSchemaListForConf();

    /**
     * 查询采用task配置的taskId
     *
     * @return
     */
    @Select("select t.id from t_biz_task t,t_biz_schema s where t.schema_id=s.id and t.if_self_conf=1 and t.available=1")
    List<Map> selectAllTaskListForConf();

    /**
     * 查询配置的最后更新时间
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = ZooConfCacheMapperProvider.class, method = "selectMaxUpdTimeForConf")
    String selectMaxUpdTimeForConf(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    /**
     * 查询当前配置中包含的stage列表
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = ZooConfCacheMapperProvider.class, method = "selectStageListForConf")
    List<String> selectStageListForConf(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    /**
     * 根据stageId获取其对应的流程节点配置
     *
     * @param schemaId
     * @param taskId
     * @param stageId
     * @return
     */
    @SelectProvider(type = ZooConfCacheMapperProvider.class, method = "selectConfListByStage")
    List<Map> selectConfListByStage(@Param("schemaId") String schemaId, @Param("taskId") String taskId, @Param("stageId") String stageId);

    @Select("select t.id task_id,t.schema_id,t.if_self_conf from t_biz_batch b left join t_biz_task t on b.task_id=t.id where b.id=#{batchId}")
    Map selectTaskAndSchemaByBatchId(String batchId);

    static class ZooConfCacheMapperProvider {

        public String selectMaxUpdTimeForConf(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("select MAX(t.upd_time) upd_time from ")
                    .append("(")
                    .append("select MAX(f.upd_time) upd_time from t_conf_flow f where %s ")
                    .append("UNION ")
                    .append("select MAX(fg.upd_time) upd_time from t_conf_flow_group fg where %s ")
                    .append(") t");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "f.schema_id=#{schemaId}", "fg.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "f.task_id=#{taskId}", "fg.task_id=#{taskId}");
            }

            return sql;
        }

        public String selectStageListForConf(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT DISTINCT sf.stage_id from ")
                    .append("(")
                    .append("select f.flow_id from t_conf_flow f where f.available=1 and %s ")
                    .append("UNION ")
                    .append("select fg.flow_id from t_conf_flow_group fg where fg.available=1 and %s ")
                    .append(") t ")
                    .append("left join t_def_stage_flow sf ")
                    .append("on t.flow_id=sf.id ")
                    .append("left join t_def_stage s ")
                    .append("on sf.stage_id=s.id ")
                    .append("order by s.order_num asc");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "f.schema_id=#{schemaId}", "fg.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "f.task_id=#{taskId}", "fg.task_id=#{taskId}");
            }

            return sql;
        }

        public String selectConfListByStage(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("select * from ")
                    .append("(")
                    .append("select * from ")
                    .append("(")
                    .append("SELECT f.flow_id,f.threadable,m.threadable threadable_def,'' param,df.flow_type,df.has_sub,df.name,df.parent_id,m.descp,m.sv_name,m.node_class,m.flow_class,m.testable,df.order_num,1 run_order_num from t_conf_flow f ")
                    .append("LEFT JOIN t_def_stage_flow df ")
                    .append("on f.flow_id=df.id ")
                    .append("left join t_def_module m ")
                    .append("on df.id=m.flow_id ")
                    .append("where %s and df.stage_id=#{stageId} and f.available=1 ")
                    .append(") t1 ")
                    .append("union ALL ")
                    .append("select * from ")
                    .append("(")
                    .append("select fg.flow_id,fg.threadable,m2.threadable threadable_def,fg.param,sf.flow_type,0 has_sub,m2.name,null parent_id,m2.descp,m2.sv_name,m2.node_class,m2.flow_class,m2.testable,sf.order_num,fg.order_num run_order_num from t_conf_flow_group fg ")
                    .append("left join t_def_module m2 ")
                    .append("on fg.module_id=m2.id ")
                    .append("left join t_def_stage_flow sf ")
                    .append("on fg.flow_id=sf.id ")
                    .append("where %s and sf.stage_id=#{stageId} and fg.available=1 ")
                    .append(") t2 ")
                    .append(") t3 order by t3.order_num asc,t3.run_order_num asc");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "f.schema_id=#{schemaId}", "fg.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "f.task_id=#{taskId}", "fg.task_id=#{taskId}");
            }

            return sql;
        }

    }
}
