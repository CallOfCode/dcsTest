package com.baiyyy.didcs.dao.invoker;

import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.Map;

/**
 * 匹配调用器 mapper
 *
 * @author 逄林
 */
public interface MatchInvokerMapper {

    /**
     * 获取待处理数据的maxId,minId和总数量total
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @return
     */
    @SelectProvider(type = MatchInvokerMapper.MatchInvokerMapperProvider.class, method = "selectMaxMinTotal")
    public Map selectMaxMinTotal(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId);

    /**
     * 分页获取每页的maxId,minId
     * @param sourceCode
     * @param taskCode
     * @param batchNum
     * @param minId
     * @param maxId
     * @param limit 分页数量
     * @return
     */
    @SelectProvider(type = MatchInvokerMapper.MatchInvokerMapperProvider.class, method = "selectMaxMinLimit")
    public Map selectMaxMinLimit(@Param("sourceCode") String sourceCode, @Param("taskCode") String taskCode, @Param("batchNum") String batchNum, @Param("minId") String minId, @Param("maxId") String maxId, @Param("limit") String limit);


    static class MatchInvokerMapperProvider {

        public String selectMaxMinTotal(Map map) {
            StringBuffer sb = new StringBuffer().append("select count(id) total,min(id) minId,max(id) maxId from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString())
                    .append(" where id>=").append(map.get("minId").toString());
            if (StringUtils.isNotBlank((String) map.get("maxId"))) {
                sb.append(" and id<=").append(map.get("maxId").toString());
            }
            sb.append(" and data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" and batch_id=").append(map.get("batchNum").toString());

            return sb.toString();
        }

        public String selectMaxMinLimit(Map map) {
            StringBuffer sb = new StringBuffer()
                    .append("select max(t.id) maxId,MIN(t.id) minId FROM ")
                    .append("(")
                    .append("select id from ").append(map.get("sourceCode").toString()).append("_").append(map.get("taskCode").toString())
                    .append(" where id>=").append(map.get("minId").toString())
                    .append(" and id<=").append(map.get("maxId").toString())
                    .append(" and data_status=").append(FlowConstant.DATA_STATUS_VALID).append(" and batch_id=").append(map.get("batchNum").toString())
                    .append(" order by id asc");
            if(!MapUtil.isBlank(map.get("limit"))){
                sb.append(" limit ").append(map.get("limit"));
            }
            sb.append(") t");

            return sb.toString();
        }

    }
}
