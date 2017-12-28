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
 * 格式化流程相关mapper
 *
 * @author 逄林
 */
@Mapper
public interface FmtFlowMapper extends BaseMapper {

    /**
     * 查询待格式化数据
     *
     * @param sourceCode 待清洗表前缀
     * @param taskCode   任务后缀
     * @param batchNum   批次编号
     * @param minId      本次最小id
     * @param maxId      最大id
     * @param pageSize   分页大小
     * @param notNullSql 非空条件
     * @return
     */
    @SelectProvider(type = FmtFlowMapper.FmtFlowMapperProvider.class, method = "selectFmtDataListPage")
    public List<HashMap> selectFmtDataListPage(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId, @Param("pageSize") Integer pageSize, @Param("notNullSql") String notNullSql);

    /**
     * 批量更新格式化后的数据
     * @param table
     * @param dataList
     */
    @UpdateProvider(type = FmtFlowMapper.FmtFlowMapperProvider.class, method = "batchUpdateDataList")
    public void batchUpdateDataList(@Param("table") String table, @Param("dataList") List<Map> dataList);

    static class FmtFlowMapperProvider {

        public String selectFmtDataListPage(Map map) {
            StringBuffer sb = new StringBuffer().append("select * from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString())
                    .append(" where id>").append(map.get("minId").toString());
            if (StringUtils.isNotBlank((String) map.get("maxId"))) {
                sb.append(" and id<=").append(map.get("maxId").toString());
            }
            sb.append(" and data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" and batch_id=").append(map.get("batchNum").toString())
                    .append(StringUtils.isBlank(map.get("notNullSql").toString()) ? "" : map.get("notNullSql").toString())
                    .append(" order by id asc LIMIT ").append(map.get("pageSize").toString());

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
