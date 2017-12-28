package com.baiyyy.didcs.interfaces.replacer;

import java.util.List;
import java.util.Map;

/**
 * 替换接口
 *
 * @author 逄林
 */
public interface IReplacer {

    /**
     * 初始化配置
     *
     * @param fields          数据字段(源字段和标准字段中的前缀部分)
     * @param replaceStrategy 替换策略
     */
    public void initParam(String fields, String replaceStrategy);

    /**
     * 替换方法（并不真正替换，而是生成需要替换的值，然后在流程中进行最终的替换）
     *
     * @param dataRow
     * @param stdRow
     * @return 返回将要替换（或通知）的字段
     */
    public List doReplace(Map dataRow, Map stdRow);

    /**
     * 获取替换策略
     * @return
     */
    public String getReplaceStrategy();
}
