package com.baiyyy.didcs.common.util;

import jodd.http.HttpRequest;
import jodd.http.HttpResponse;

import java.util.Iterator;
import java.util.Map;

/**
 * jodd http访问工具类
 *
 * @author 逄林
 */
public class JoddHttpUtil {

    /**
     * Get请求
     * @param url
     * @param params
     * @return
     */
    public static HttpResponse doGet(String url,Map<String,String> params){
        HttpRequest request = HttpRequest.get(url);
        if(null!=params&&!params.isEmpty()){
            request.query(params);
        }
        return request.send();
    }

    /**
     * Post请求
     * @param url
     * @param params
     * @return
     */
    public static HttpResponse doPost(String url,Map<String,Object> params){
        HttpRequest request = HttpRequest.post(url);
        if(null!=params&&!params.isEmpty()){
            request.form(params);
        }
        return request.send();
    }

}
