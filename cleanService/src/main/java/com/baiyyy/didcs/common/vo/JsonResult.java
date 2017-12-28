package com.baiyyy.didcs.common.vo;

import java.io.Serializable;

/**
 * json结果，用作结果传递
 *
 * @param <E>
 * @author 逄林
 */
public class JsonResult<E> implements Serializable {
    private static final long serialVersionUID = 3744153138505703077L;

    public static Boolean RESULT_SUCCESS_CODE = true;
    public static Boolean RESULT_FAILURE_CODE = false;

    /**
     * 返回结果
     */
    private Boolean result;
    /**
     * 返回代码
     */
    private String code;
    /**
     * 返回消息
     */
    private String msg;
    /**
     * 返回数据
     */
    private E data;

    public Boolean getResult() {
        return result;
    }

    public void setResult(Boolean result) {
        this.result = result;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public E getData() {
        return data;
    }

    public void setData(E data) {
        this.data = data;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer().append("{")
                .append("code:")
                .append(getCode())
                .append(",")
                .append("msg:")
                .append(getMsg())
                .append(",")
                .append("result:")
                .append(getData().toString())
                .append("}");

        return sb.toString();
    }
}
