package com.baiyyy.didcs.dao;

import com.baiyyy.didcs.common.util.MapUtil;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;
import java.util.Map;

@Mapper
public interface SchemaMapper extends BaseMapper{
    @SelectProvider(type = SchemaMapperProvider.class, method = "selectPageSchemas")
    public List<Map> selectPageSchemas(@Param("name") String name,@Param("limit") Integer limit,@Param("offset") Integer offset);

    static class SchemaMapperProvider {
        public String selectPageSchemas(Map map) {
            StringBuffer sb = new StringBuffer();
            sb.append("select s.* from t_biz_schema s where s.available=1 ");
            if(MapUtil.isNotBlank(map.get("name"))){
                sb.append("and s.name like CONCAT('%',#{name},'%') ");
            }
            sb.append("order by id asc ")
            .append("LIMIT #{limit} OFFSET #{offset}");
            return sb.toString();
        }
    }
}
