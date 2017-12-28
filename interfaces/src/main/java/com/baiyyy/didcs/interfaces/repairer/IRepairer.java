package com.baiyyy.didcs.interfaces.repairer;

import java.util.List;

/**
 * 字段补缺接口
 *
 * @author 逄林
 */
public interface IRepairer {

    /**
     * 初始化补缺参数
     *
     * @param sourceFields 缺失字段
     * @param destFields   用于补缺的字段
     */
    public void initParam(String sourceFields, String destFields);

    /**
     * 执行补缺
     *
     * @param dataRow
     * @return 补缺dataRow并返回更新列表
     */
    public List doRepair(Object dataRow);

}
