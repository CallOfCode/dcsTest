package com.baiyyy.didcs.dao.flow;

import com.baiyyy.didcs.common.constant.ArchiveConstant;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.DateTimeUtil;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.BaseMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.*;

import java.util.List;
import java.util.Map;

/**
 * 归档相关Mapper
 *
 * @author 逄林
 */
@Mapper
public interface ArchiveMapper extends BaseMapper{

    @SelectProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "selectArchiveConf")
    public List<Map> getArchiveConfByBatchId(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    /**
     * 获取待处理数据的maxId,minId和总数量total
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param sqlMap
     * @return
     */
    @SelectProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "selectMaxMinTotal")
    public Map selectMaxMinTotal(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId, @Param("sqlMap") Map sqlMap);

    /**
     * 分页获取每页的maxId,minId
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param limit 分页数量
     * @param sqlMap
     * @return
     */
    @SelectProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "selectMaxMinLimit")
    public Map selectMaxMinLimit(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId,@Param("limit") String limit, @Param("sqlMap") Map sqlMap);

    /**
     * 获取限定范围内的id
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param limit 分页数量
     * @param sqlMap
     * @return
     */
    @SelectProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "selectIdsWithMaxMinLimit")
    public List<String> selectIdsWithMaxMinLimit(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId,@Param("limit") String limit, @Param("sqlMap") Map sqlMap);

    /**
     * 根据id进行数据归档
     * @param sourceTable
     * @param sourceFields
     * @param stdTable
     * @param stdFields
     * @param ids
     */
    @InsertProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "insertStdTableFromSource")
    public void insertStdTableFromSource(@Param("sourceTable") String sourceTable,@Param("sourceFields") String sourceFields,@Param("stdTable") String stdTable,@Param("stdFields") String stdFields,@Param("ids") String ids);

    @SelectProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "getStdIdsBySourceIds")
    public List<String> getStdIdsBySourceIds(@Param("stdTable") String stdTable,@Param("batchId") String batchId,@Param("ids") String ids);

    /**
     * 根据id进行数据状态更新
     * @param sourceTable
     * @param ids
     */
    @UpdateProvider(type = ArchiveMapper.ArchiveMapperProvider.class, method = "updateSourceTableStatus")
    public void updateSourceTableStatus(@Param("sourceTable") String sourceTable,@Param("ids") String ids);

    static class ArchiveMapperProvider {

        public String selectArchiveConf(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT s.* from t_conf_archive s ")
                    .append("where %s ");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "s.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "s.task_id=#{taskId}");
            }

            return sql;
        }

        public String selectMaxMinTotal(Map map) {
            StringBuffer sb = new StringBuffer().append("select count(t1.id) total,max(t1.id) maxId,min(t1.id) minId from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString())
                    .append(" t1 where t1.id>=").append(map.get("minId").toString());
            if (StringUtils.isNotBlank((String) map.get("maxId"))) {
                sb.append(" and t1.id<=").append(map.get("maxId").toString());
            }
            sb.append(" and data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" and batch_id=").append(map.get("batchNum").toString())
                    .append((map.containsKey("sqlMap")&&((Map)map.get("sqlMap")).containsKey(ArchiveConstant.CONF_TYPE_NOT_NULL))?((Map)map.get("sqlMap")).get(ArchiveConstant.CONF_TYPE_NOT_NULL):"")
                    .append(" and not EXISTS(")
                    .append("select d.id from t_std_doctor d ")
                    .append("where ")
                    .append((map.containsKey("sqlMap")&&((Map)map.get("sqlMap")).containsKey(ArchiveConstant.CONF_TYPE_NOT_REPEAT))?((Map)map.get("sqlMap")).get(ArchiveConstant.CONF_TYPE_NOT_REPEAT):"")
                    .append(")");
            return sb.toString();
        }

        public String selectMaxMinLimit(Map map) {
            StringBuffer sb = new StringBuffer().append("select max(t1.id) maxId,min(t1.id) minId from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString())
                    .append("t1 where t1.id>=").append(map.get("minId").toString());
            if (StringUtils.isNotBlank((String) map.get("maxId"))) {
                sb.append(" and t1.id<=").append(map.get("maxId").toString());
            }
            sb.append(" and data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" and batch_id=").append(map.get("batchNum").toString())
                    .append((map.containsKey("sqlMap")&&((Map)map.get("sqlMap")).containsKey(ArchiveConstant.CONF_TYPE_NOT_NULL))?((Map)map.get("sqlMap")).get(ArchiveConstant.CONF_TYPE_NOT_NULL):"")
                    .append(" and not EXISTS(")
                    .append("select d.id from t_std_doctor d ")
                    .append("where ")
                    .append((map.containsKey("sqlMap")&&((Map)map.get("sqlMap")).containsKey(ArchiveConstant.CONF_TYPE_NOT_REPEAT))?((Map)map.get("sqlMap")).get(ArchiveConstant.CONF_TYPE_NOT_REPEAT):"")
                    .append(")")
                    .append(" order by t1.id ASC ");

            if(!MapUtil.isBlank(map.get("limit"))){
                sb.append(" limit ").append(map.get("limit"));
            }
            return sb.toString();
        }

        public String selectIdsWithMaxMinLimit(Map map) {
            StringBuffer sb = new StringBuffer().append("select t1.id from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString())
                    .append(" t1 where t1.id>=").append(map.get("minId").toString());
            if (StringUtils.isNotBlank((String) map.get("maxId"))) {
                sb.append(" and t1.id<=").append(map.get("maxId").toString());
            }
            sb.append(" and data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" and batch_id=").append(map.get("batchNum").toString())
                    .append((map.containsKey("sqlMap")&&((Map)map.get("sqlMap")).containsKey(ArchiveConstant.CONF_TYPE_NOT_NULL))?((Map)map.get("sqlMap")).get(ArchiveConstant.CONF_TYPE_NOT_NULL):"")
                    .append(" and not EXISTS(")
                    .append("select d.id from t_std_doctor d ")
                    .append("where ")
                    .append((map.containsKey("sqlMap")&&((Map)map.get("sqlMap")).containsKey(ArchiveConstant.CONF_TYPE_NOT_REPEAT))?((Map)map.get("sqlMap")).get(ArchiveConstant.CONF_TYPE_NOT_REPEAT):"")
                    .append(")")
                    .append(" order by t1.id ASC ");

            if(!MapUtil.isBlank(map.get("limit"))){
                sb.append(" limit ").append(map.get("limit"));
            }
            return sb.toString();
        }

        public String insertStdTableFromSource(Map map) {
            StringBuffer sb = new StringBuffer().append("insert into ")
                    .append(map.get("stdTable")).append("(").append(map.get("stdFields")).append(") ")
                    .append("select ").append(map.get("sourceFields"))
                    .append(" from ").append(map.get("sourceTable"))
                    .append(" where id in (").append(map.get("ids")).append(")");
            return sb.toString();
        }

        public String updateSourceTableStatus(Map map) {
            StringBuffer sb = new StringBuffer().append("update ").append(map.get("sourceTable"))
                    .append(" set data_status=").append(FlowConstant.DATA_STATUS_STO).append(",upd_time='").append(DateTimeUtil.getNowYYYYMMDDHHMMSS()).append("'")
                    .append(" where id in (").append(map.get("ids")).append(")");
            return sb.toString();
        }

        public String getStdIdsBySourceIds(Map map){
            StringBuffer sb = new StringBuffer().append("select id from ").append(map.get("stdTable"))
                    .append(" where source_batch_id=").append(map.get("batchId"))
                    .append(" and source_data_id in (").append(map.get("ids")).append(")");
            return sb.toString();
        }
    }
}
