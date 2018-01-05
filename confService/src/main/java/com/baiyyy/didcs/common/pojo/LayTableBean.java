package com.baiyyy.didcs.common.pojo;

import java.io.Serializable;
import java.util.List;

public class LayTableBean implements Serializable {
    private static final long serialVersionUID = 7247714666080613254L;
    private String msg;
    private int code;
    private List data;
    private int count;

    public LayTableBean() {
        super();
    }

    public LayTableBean(int code, String msg, int count, List data) {
        this.msg = msg;
        this.code = code;
        this.data = data;
        this.count = count;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public List getData() {
        return data;
    }

    public void setData(List data) {
        this.data = data;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
