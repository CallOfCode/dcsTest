package com.baiyyy.didcs.common.util;

import org.joda.time.DateTime;

import java.util.Date;

/**
 * 时间工具类
 *
 * @author 逄林
 */
public class DateTimeUtil {

    public static String getNowYYYYMMDDHHMMSS(){
        DateTime dateTime = new DateTime();
        return dateTime.toString("yyyy-MM-dd HH:mm:ss");
    }

    public static String getYYYYMMDDHHMMSS(Long mills){
        DateTime dateTime = new DateTime(new Date(mills));
        return dateTime.toString("yyyy-MM-dd HH:mm:ss");
    }

}
