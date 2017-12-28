package com.baiyyy.didcs.interfaces.standarder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Map;

/**
 * 标准化接口，使用ES进行数据标准化
 *
 * @author 逄林
 */
public interface IStandarder {

    /**
     * 初始化Client
     *
     * @param client
     */
    public void setClient(RestHighLevelClient client);

    /**
     * 设置index名称
     * @param index
     */
    public void setIndex(String index);

    /**
     * 标准化
     * @param element 被标准化值
     * @param refElementFiledName  引用字段名称，多个以逗号分隔
     * @param dataMap 对应的数据行，当有引用字段是才需要传入
     * @param aliasSys 别名sys值
     * @return 返回值 [0]=文字值 [1]=id,返回null代表无值
     */
    public String[] doStd(String element, String refElementFiledName, Map<String,Object> dataMap, String aliasSys);

}
