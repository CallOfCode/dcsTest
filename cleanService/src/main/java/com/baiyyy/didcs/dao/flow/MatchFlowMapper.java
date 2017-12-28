package com.baiyyy.didcs.dao.flow;

import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.DateTimeUtil;
import com.baiyyy.didcs.dao.BaseMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 匹配流程相关mapper
 *
 * @author 逄林
 */
@Mapper
public interface MatchFlowMapper extends BaseMapper{

    /**
     * 查询待匹配数据
     *
     * @param sourceCode 待清洗表前缀
     * @param taskCode   任务后缀
     * @param batchNum   批次编号
     * @param minId      本次最小id
     * @param maxId      最大id
     * @param pageSize   分页大小
     * @return
     */
    @SelectProvider(type = MatchFlowMapper.MatchFlowMapperProvider.class, method = "selectNeddMatchDataListPage")
    public List<HashMap> selectNeddMatchDataListPage(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId, @Param("pageSize") Integer pageSize);

    /**
     * 批量更新格式化后的数据
     * @param table
     * @param dataList
     */
    @UpdateProvider(type = MatchFlowMapper.MatchFlowMapperProvider.class, method = "batchUpdateDataList")
    public void batchUpdateDataList(@Param("table") String table, @Param("dataList") List<Map> dataList);

    /**
     * 批量更新标准库数据
     * @param table
     * @param dataList
     */
    @UpdateProvider(type = MatchFlowMapper.MatchFlowMapperProvider.class, method = "batchUpdateStdDataList")
    public void batchUpdateStdDataList(@Param("table") String table, @Param("dataList") List<Map> dataList);

    @InsertProvider(type = MatchFlowMapper.MatchFlowMapperProvider.class, method = "batchInsertNotifyDataList")
    public void batchInsertNotifyDataList(@Param("dataList") List<Map> dataList);

    static class MatchFlowMapperProvider {

        public String selectNeddMatchDataListPage(Map map) {
            StringBuffer sb = new StringBuffer().append("select c.*,t.not_match_id from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString()).append(" c ")
                    .append("left join (select data_id,GROUP_CONCAT(not_match_id) not_match_id from t_biz_match_notlike where batch_id=").append(map.get("batchNum").toString()).append(" GROUP BY data_id ) t on c.id=t.data_id")
                    .append(" where c.id>").append(map.get("minId").toString());
            if (StringUtils.isNotBlank((String) map.get("maxId"))) {
                sb.append(" and c.id<=").append(map.get("maxId").toString());
            }
            sb.append(" and (c.data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" or c.data_status=").append(FlowConstant.DATA_STATUS_LIKE).append(") and c.batch_id=").append(map.get("batchNum").toString())
                    .append(" order by c.id asc LIMIT ").append(map.get("pageSize").toString());

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

        public String batchUpdateStdDataList(Map map){
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

        public String batchInsertNotifyDataList(Map map){
            StringBuffer sb = new StringBuffer();
            List<Map> dataList = (List<Map>)map.get("dataList");
            String updTime = DateTimeUtil.getNowYYYYMMDDHHMMSS();
            if(null!=dataList){
                sb.append("INSERT into t_biz_std_update(table_name,data_id,attr_name,value_text,value_id,source_data_id,source_batch_id,upd_time) ")
                        .append("values ");
                for(int i=0;i<dataList.size();i++){
                    if(i>0){
                        sb.append(",");
                    }
                    sb.append("(")
                            .append("#{dataList[").append(i).append("].").append("table_name").append("},")
                            .append("#{dataList[").append(i).append("].").append("data_id").append("},")
                            .append("#{dataList[").append(i).append("].").append("attr_name").append("},")
                            .append("#{dataList[").append(i).append("].").append("new_strval").append("},")
                            .append("#{dataList[").append(i).append("].").append("new_idval").append("},")
                            .append("#{dataList[").append(i).append("].").append("ref_data_id").append("},")
                            .append("#{dataList[").append(i).append("].").append("batch_id").append("},")
                            .append("'").append(DateTimeUtil.getNowYYYYMMDDHHMMSS()).append("'")
                            .append(")");
                }
            }
            return sb.toString();
        }
    }
}
