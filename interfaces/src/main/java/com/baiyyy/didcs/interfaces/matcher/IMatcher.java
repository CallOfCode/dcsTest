package com.baiyyy.didcs.interfaces.matcher;

import org.elasticsearch.client.RestHighLevelClient;

/**
 * 匹配接口
 *
 * @author 逄林
 */
public interface IMatcher {

    /**
     * 初始化参数
     *
     * @param fields   待清洗表匹配字段
     * @param stdFields 标准表匹配字段
     * @param matchTye 匹配类型
     * @param params   其他参数
     * @param cacheCode   缓存代码
     * @param geoField   地理信息字段名称
     */
    public void initParam(String fields, String stdFields, String matchTye, String params, String cacheCode, String geoField);

    /**
     * 执行配置
     *
     * @param client
     * @param dataRow
     * @return 返回结果为匹配中的标准数据的ID，模糊匹配可为多个
     */
    public String doMatch(RestHighLevelClient client, Object dataRow);

    /**
     * 获取匹配器类型
     * @return
     */
    public String getMatcherType();
}
