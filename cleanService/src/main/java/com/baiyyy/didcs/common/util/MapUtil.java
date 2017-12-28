package com.baiyyy.didcs.common.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Map操作工具类
 *
 * @author 逄林
 */
public class MapUtil {

    /**
     * 获取字符串，如果为NULL则转化为''
     * @param str
     * @return
     */
    public static String getCasedString(Object str){
        return getCasedString(str,"");
    }

    /**
     * 获取字符串，转化为整数
     * @param str
     * @return
     */
    public static Integer getCasedInteger(Object str){
        return getCasedInteger(str,null);
    }

    /**
     * 获取字符串，如果为NULL则转化为默认字符串
     * @param str
     * @param defaultStr
     * @return
     */
    public static String getCasedString(Object str,String defaultStr){
        return null==str?defaultStr:String.valueOf(str);
    }

    /**
     * 获取字数字，如果为NULL则转化为数字
     * @param str
     * @param defaultVal
     * @return
     */
    public static Integer getCasedInteger(Object str,Integer defaultVal){
        if(isBlank(str)){
            return defaultVal;
        }else{
            return Integer.valueOf(getCasedString(str));
        }
    }

    /**
     * 判断是否为空或''
     * @param str
     * @return
     */
    public static Boolean isBlank(Object str){
        return null==str?true: StringUtils.isBlank(str.toString());
    }

    public static Boolean isNotBlank(Object str){
        return !isBlank(str);
    }

}
