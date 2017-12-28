package com.baiyyy.didcs.dao.es;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

/**
 * Es别名Mapper
 *
 * @author 逄林
 */
@Mapper
public interface EsAliasMapper {

    /**
     * 查询别名表中的sys分类
     * @param tableName
     * @return
     */
    @Select("SELECT DISTINCT IFNULL(t.sys,'common') sys from t_std_${tableName}_alias t")
    public List<String> getAliasSys(@Param("tableName") String tableName);

    /**
     * 查询在指定时间后出现的sys分类
     * @param tableName
     * @param updTime
     * @return
     */
    @Select("select DISTINCT IFNULL(sys,'common') sys from t_std_${tableName}_alias " +
            "where upd_time>#{updTime}")
    public List<String> getAliasSysByUpdTime(@Param("tableName") String tableName,@Param("updTime") String updTime);

    /**
     * 分页查询标准化数据
     * @param tableName
     * @param minId
     * @param limit
     * @return
     */
    @SelectProvider(type = EsAliasMapper.EsAliasMapperProvider.class, method = "getStdDataWithAlias")
    public List<Map> getStdDataWithAlias(@Param("tableName") String tableName,@Param("minId") Integer minId,@Param("limit") Integer limit);

    /**
     * 根据时间获取需要更新的数据的id
     * @param tableName
     * @param updTime
     * @return
     */
    @SelectProvider(type = EsAliasMapper.EsAliasMapperProvider.class, method = "getNeedUpdDataByUpdTime")
    public List<String> getNeedUpdDataByUpdTime(@Param("tableName") String tableName,@Param("updTime") String updTime);

    /**
     * 根据id获取带别名的标准数据
     * @param tableName
     * @param ids
     * @return
     */
    @SelectProvider(type = EsAliasMapper.EsAliasMapperProvider.class, method = "getStdDataWithAliasByIds")
    public List<Map> getStdDataWithAliasByIds(@Param("tableName") String tableName,@Param("ids") String ids);

    /**
     * 根据cacheCode获取标准表的schema attr配置
     * @param cacheCode
     * @return
     */
    @Select("select a.* from t_def_stds s,t_def_stds_attr a where s.cache_code=#{cacheCode} " +
            "and s.id = a.stds_id " +
            "order by a.order_num asc")
    public List<Map> getStdSchemaAttrByCode(@Param("cacheCode") String cacheCode);

    /**
     * 根据cacheCode获取标准表的多值字段
     * @param cacheCode
     * @return
     */
    @Select("select a.code,a.sp_char,a.if_std from t_def_stds s,t_def_stds_attr a where s.cache_code=#{cacheCode} " +
            "and s.id = a.stds_id and a.if_multi=1 " +
            "order by a.order_num asc")
    public List<Map> getStdSchemaMultiAttrByCode(@Param("cacheCode") String cacheCode);

    static class EsAliasMapperProvider {

        public String getStdDataWithAlias(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("select m.*,a.alias_name from t_std_").append(map.get("tableName")).append(" m ")
                    .append("left join ")
                    .append("( ")
                    .append("SELECT t.std_id,GROUP_CONCAT(t.alias_name SEPARATOR '_,_') alias_name ")
                    .append("from ")
                    .append("( ")
                    .append("select std_id,CONCAT(IFNULL(sys,'common'),'$',alias_name) alias_name from t_std_").append(map.get("tableName")).append("_alias ")
                    .append(") t ")
                    .append("group by t.std_id ")
                    .append(") a ")
                    .append("on m.id=a.std_id ")
                    .append("where m.id>").append(map.get("minId")).append(" ")
                    .append("order by m.id asc ")
                    .append("limit ").append(map.get("limit"));

            String sql = sb.toString();
            return sql;
        }

        public String getNeedUpdDataByUpdTime(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT DISTINCT t1.id FROM ")
                    .append(map.get("tableName")).append(" t1 ")
                    .append("left JOIN ")
                    .append(map.get("tableName")).append("_alias t2 ")
                    .append("on t1.id = t2.std_id ")
                    .append("WHERE ")
                    .append("t1.add_time>'%s' or t1.upd_time>'%s' or t2.upd_time>'%s' ")
                    .append("ORDER BY t1.id asc");

            String sql = sb.toString();
            sql = String.format(sql,map.get("updTime"),map.get("updTime"),map.get("updTime"));
            return sql;
        }

        public String getStdDataWithAliasByIds(Map map){
            StringBuffer sb = new StringBuffer();
            sb.append("select m.*,a.alias_name from ")
                    .append("(")
                    .append("SELECT DISTINCT t1.id from t_std_").append(map.get("tableName")).append(" t1 left join t_std_").append(map.get("tableName")).append("_alias t2 on t1.id = t2.std_id ")
                    .append("where t1.id in (").append(map.get("ids")).append(") ")
                    .append(") t ")
                    .append("LEFT JOIN ")
                    .append("t_std_").append(map.get("tableName")).append(" m ")
                    .append("on t.id=m.id ")
                    .append("left join ")
                    .append("( ")
                    .append("SELECT t.std_id,GROUP_CONCAT(t.alias_name SEPARATOR '_,_') alias_name ")
                    .append("from ")
                    .append("( ")
                    .append("select std_id,CONCAT(IFNULL(sys,'common'),'$',alias_name) alias_name from t_std_").append(map.get("tableName")).append("_alias where std_id in (").append(map.get("ids")).append(")")
                    .append(") t ")
                    .append("group by t.std_id ")
                    .append(") a ")
                    .append("on m.id=a.std_id ")
                    .append("order by m.id asc ");

            return sb.toString();
        }

    }
}
