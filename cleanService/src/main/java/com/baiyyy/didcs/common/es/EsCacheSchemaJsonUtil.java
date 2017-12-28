package com.baiyyy.didcs.common.es;

import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.dao.es.EsAliasMapper;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * ES schema json获取
 *
 * @author 逄林
 */
public class EsCacheSchemaJsonUtil {

    /**
     * 获取创建Index的mapping json字符串
     * @param name
     * @return
     */
    public static String getIndexBody(String name){
        return getEsBody(JSONObject.parseObject(getMappingByName(name))).toJSONString();
    }

    /**
     * 获取系统用的缓存的body,name为index的全名
     * @param name
     * @return
     */
    public static String getSysIndexBody(String name){
        String body = getSysMappingByName(name);
        if(StringUtils.isNotBlank(body)){
            return getEsBody(JSONObject.parseObject(body)).toJSONString();
        }else {
            return null;
        }
    }

    private static JSONObject getEsBody(JSONObject docProperty){
        JSONObject body = new JSONObject();
        JSONObject setting = new JSONObject();
        setting.put("number_of_shards",1);
        setting.put("number_of_replicas",1);
        body.put("settings",setting);

        JSONObject mapping = new JSONObject();
        JSONObject doc = new JSONObject();
        if(null==docProperty){
            doc.put("properties",new JSONObject());
        }else{
            doc.put("properties",docProperty);
        }

        mapping.put("doc",doc);
        body.put("mappings",mapping);
        return body;
    }

    private static String getMappingByName(String name){
        StringBuffer sb = new StringBuffer();
        //1.根据名称获取标准表结构
        EsAliasMapper esAliasMapper = SpringContextUtil.getBean("esAliasMapper");
        List<Map> list = esAliasMapper.getStdSchemaAttrByCode(name);
        sb.append("{");
        for(Map field:list){
            Integer ifStd = MapUtil.getCasedInteger(field.get("if_std"));
            if(1==ifStd){
                sb.append("\"").append(MapUtil.getCasedString(field.get("code"))).append("\": {")
                        .append("\"type\": \"").append(MapUtil.getCasedString(field.get("cache_type"))).append("\"")
                        .append("},");
                sb.append("\"").append(MapUtil.getCasedString(field.get("code"))).append("_id\": {")
                        .append("\"type\": \"").append("integer").append("\"")
                        .append("},");
            }else{
                sb.append("\"").append(MapUtil.getCasedString(field.get("code"))).append("\": {")
                        .append("\"type\": \"").append(MapUtil.getCasedString(field.get("cache_type"))).append("\"")
                        .append("},");
            }
        }
        sb.append(getAliasStr(name));
        sb.append("}");
        return sb.toString();
    }

    private static String getAliasStr(String name){
        StringBuffer sb = new StringBuffer();
        //获取别名分类
        EsAliasMapper esAliasMapper = SpringContextUtil.getBean("esAliasMapper");
        List<String> aliasStringList = esAliasMapper.getAliasSys(name);
        if(null==aliasStringList || aliasStringList.size()==0){
            sb.append("\"alias_common\": {")
                    .append("\"type\": \"keyword\",")
                    .append("\"index\": \"true\"")
                    .append("}");
        }else{
            for(int i=0;i<aliasStringList.size();i++){
                if(i==0){
                    sb.append("\"alias_")
                            .append(aliasStringList.get(i))
                            .append("\": {")
                            .append("\"type\": \"keyword\",")
                            .append("\"index\": \"true\"")
                            .append("}");
                }else{
                    sb.append(",\"alias_")
                            .append(aliasStringList.get(i))
                            .append("\": {")
                            .append("\"type\": \"keyword\",")
                            .append("\"index\": \"true\"")
                            .append("}");
                }
            }
        }
        return sb.toString();
    }

    private static String getSysMappingByName(String name){
        StringBuffer sb = new StringBuffer();
        if(name.startsWith(EsConstant.INDEX_PREFIX_CLEAN_LOG)){
            sb.append("{")
                    .append("\"batch_id\":{")
                    .append("\"type\":\"integer\"")
                    .append("},")
                    .append("\"data_id\":{")
                    .append("\"type\":\"integer\"")
                    .append("},")
                    .append("\"col\":{")
                    .append("\"type\":\"keyword\"")
                    .append("},")
                    .append("\"old_val\":{")
                    .append("\"type\":\"text\",")
                    .append("\"index\":\"false\"")
                    .append("},")
                    .append("\"new_val\":{")
                    .append("\"type\":\"text\",")
                    .append("\"index\":\"false\"")
                    .append("},")
                    .append("\"stage\":{")
                    .append("\"type\":\"text\",")
                    .append("\"index\":\"false\"")
                    .append("},")
                    .append("\"target\":{")
                    .append("\"type\":\"text\",")
                    .append("\"index\":\"false\"")
                    .append("},")
                    .append("\"user_id\":{")
                    .append("\"type\":\"text\",")
                    .append("\"index\":\"false\"")
                    .append("},")
                    .append("\"msg\":{")
                    .append("\"type\":\"text\",")
                    .append("\"index\":\"false\"")
                    .append("},")
                    .append("\"@timestamp\":{")
                    .append("\"type\":\"date\"")
                    .append("}")
                    .append("}");
        }

        return sb.toString();
    }

}
