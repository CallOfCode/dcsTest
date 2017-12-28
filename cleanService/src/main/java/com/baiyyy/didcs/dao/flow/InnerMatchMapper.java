package com.baiyyy.didcs.dao.flow;

import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.DateTimeUtil;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.BaseMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 查重mapper
 *
 * @author 逄林
 */
@Mapper
public interface InnerMatchMapper extends BaseMapper {

    /**
     * 获取查重配置
     * @param schemaId
     * @param taskId
     * @return
     */
    @SelectProvider(type = InnerMatchMapper.InnerMatchMapperProvider.class, method = "selectInnerMatchConf")
    public List<Map> selectInnerMatchConf(@Param("schemaId") String schemaId, @Param("taskId") String taskId);

    /**
     * 获取分组
     * @param tableName
     * @param batchId
     * @param fields
     * @param notNullSql
     * @return
     */
    @Select("select ${fields},count(id) count from ${tableName} where batch_id=#{batchId} ${notNullSql} group by ${fields} HAVING count>1")
    public List<Map> selectMatchGroups(@Param("tableName") String tableName,@Param("batchId") String batchId,@Param("fields") String fields,@Param("notNullSql") String notNullSql);

    /**
     * 获取分组数据
     * @param tableName
     * @param fields
     * @param group
     * @return
     */
    @SelectProvider(type = InnerMatchMapper.InnerMatchMapperProvider.class, method = "selectGroupDataList")
    public List<Map> selectGroupDataList(@Param("tableName") String tableName,@Param("fields") String fields,@Param("group") Map group,@Param("batchId") String batchId);

    /**
     * 更新数据状态
     * @param table
     * @param dataList
     */
    @UpdateProvider(type = InnerMatchMapper.InnerMatchMapperProvider.class, method = "batchUpdateDataList")
    public void batchUpdateDataList(@Param("table") String table, @Param("dataList") List<Map> dataList);

    static class InnerMatchMapperProvider {

        public String selectInnerMatchConf(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT * from t_conf_innermatch c ")
                    .append("where %s ")
                    .append("order by order_num asc");
            String sql = sb.toString();
            if (StringUtils.isNotBlank((String) map.get("schemaId"))) {
                sql = String.format(sql, "c.schema_id=#{schemaId}");
            } else {
                sql = String.format(sql, "c.task_id=#{taskId}");
            }

            return sql;
        }

        public String selectGroupDataList(Map map){
            StringBuffer sb = new StringBuffer();
            sb.append("select t.id,t.data_status from ").append(map.get("tableName")).append(" t ")
                    .append("where t.batch_id=#{batchId} ");
            String [] fields = MapUtil.getCasedString(map.get("fields")).split(",");
            for(String field : fields){
                sb.append("and ").append(field).append("=#{group.").append(field).append("} ");
            }
            sb.append("and EXISTS(")
                    .append("SELECT t2.id from ").append(map.get("tableName")).append(" t2 where t.id=t2.id and t2.data_status=")
                    .append(FlowConstant.DATA_STATUS_VALID)
                    .append(")");
            return sb.toString();
        }

        public String batchUpdateDataList(Map map){
            StringBuffer sb = new StringBuffer();
            List<Map> dataList = (List<Map>)map.get("dataList");
            String updTime = DateTimeUtil.getNowYYYYMMDDHHMMSS();
            if(null!=dataList){
                for(int i=0;i<dataList.size();i++){
                    sb.append("update ").append(map.get("table")).append(" set upd_time='").append(updTime).append("'");
                    Map data = dataList.get(i);
                    Iterator<String> it = data.keySet().iterator();
                    while (it.hasNext()){
                        String field = it.next();
                        if("id".equals(field)){
                            continue;
                        }else{
                            sb.append(",").append(field).append("=").append("#{dataList[").append(i).append("].").append(field).append("}");
                        }
                    }
                    sb.append(" where id=#{dataList[").append(i).append("].id}").append(";");
                }
            }
            return sb.toString();
        }
    }
}
