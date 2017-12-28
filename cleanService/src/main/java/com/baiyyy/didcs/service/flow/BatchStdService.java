package com.baiyyy.didcs.service.flow;

import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.dao.flow.BatchStdMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 批量标准化服务方法
 *
 * @author 逄林
 */
@Service
public class BatchStdService {

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private BatchStdMapper batchStdMapper;

    /**
     * 根据批次ID获取需要被格式化的字段
     *
     * @param batchId
     * @return
     */
    public List<Map> getNeedStdFieldsByBatchId(String batchId) {
        return batchStdMapper.getNeedStdFieldsByBatchId(batchId);
    }

    public void execUpdSqls(List<String> sqls) {
        for (String sql : sqls) {
            try {
                batchStdMapper.updateBySql(sql);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取sql
     *
     * @param tablePrefix 待清洗表前缀
     * @param tableCode   待清洗表后缀
     * @param batchCode   批次编码
     * @param field       属性字段
     * @param stdTable    标准数据表
     * @param ifCust      是否采用自定义别名
     * @param ifStdUseAlias 是否采用别名进行标准化,只用在标准化配置中进行了配置的字段才会使用别名进行标准化，没有配置过的字段，只使用标准表进行标准化
     * @return
     */
    public List<String> getSqls(String tablePrefix, String tableCode, String batchCode, String field, String stdTable, String ifCust, String systemCode, Boolean ifStdUseAlias) {
        List<String> sqls = new ArrayList<>();
        //获取批量更新sql
        String sql = new StringBuffer()
                .append("UPDATE ").append(tablePrefix).append("_").append(tableCode).append(" c,")
                .append("(select name,MIN(id) id,count(id) total from ").append(stdTable).append(" group by name)").append(" s ")
                .append("set c.").append(field).append("_stdstr=s.name,c.").append(field).append("_stdid=s.id ")
                .append("where c.").append(field).append("=s.name and c.").append(field).append("_stdid is null and s.total=1 and c.batch_id=").append(batchCode).toString();

        sqls.add(sql);

        if(ifStdUseAlias){
            String aliasSql = new StringBuffer()
                    .append("UPDATE ").append(tablePrefix).append("_").append(tableCode).append(" c inner join ")
                    .append("(select a1.alias_name,MIN(a1.std_id) std_id,COUNT(a1.id) total from ").append(stdTable).append("_alias a1 ")
                    .append("where a1.sys").append("1".equals(ifCust) ? "='" + systemCode+"'" : " is null").append(" group by a1.alias_name) a ")
                    .append("on c.").append(field).append("=a.alias_name INNER JOIN ").append(stdTable).append(" s on a.std_id=s.id ")
                    .append("set c.").append(field).append("_stdstr=s.name,c.").append(field).append("_stdid=s.id ")
                    .append("where c.").append(field).append("_stdid is null and a.total=1").append(" and c.batch_id=").append(batchCode).toString();
            sqls.add(aliasSql);
        }
        return sqls;
    }

    /**
     * 获取批次对应的字段标准化配置，主要用于判断字段是否启用个性化别名
     * @param batchId
     * @return
     */
    public Map<String,String> getFieldStdConfMap(String batchId){
        Map idMap = batchStdMapper.selectIdInfoByBatchId(batchId);
        String schemaId = MapUtil.getCasedString(idMap.get("schema_id"));
        String taskId = MapUtil.getCasedString(idMap.get("task_id"));
        String ifTaskConf = MapUtil.getCasedString(idMap.get("if_self_conf"));
        if(ConfConstant.IF_TASK_CONF.equals(ifTaskConf)){
            schemaId = null;
        }

        List<Map> fieldMap = batchStdMapper.selectFieldStdConf(schemaId,taskId);
        Map retMap = new HashMap();
        for(Map map:fieldMap){
            retMap.put(MapUtil.getCasedString(map.get("code")),MapUtil.getCasedString(map.get("if_std_cust")));
        }
        return retMap;
    }

}
