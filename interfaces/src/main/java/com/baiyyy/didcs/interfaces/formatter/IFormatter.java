package com.baiyyy.didcs.interfaces.formatter;

import java.util.List;

/**
 * 格式化执行器接口
 *
 * @param <E>
 * @author 逄林
 */
public interface IFormatter<E> {
    /**
     * 初始化规则
     *
     * @param rules
     */
    public void initRules(List rules);

    /**
     * 执行格式化方法
     *
     * @param element 被格式化值
     * @return 格式化值
     */
    public E doFormat(E element);
}
