package com.baiyyy.didcs.interfaces.validator;

/**
 * 校验程序接口
 *
 * @author 逄林
 */
public interface IValidator<E> {

    /**
     * 数据校验
     *
     * @param element
     * @return 校验结果，结果为true false
     */
    public Boolean doValidate(E element);

}
