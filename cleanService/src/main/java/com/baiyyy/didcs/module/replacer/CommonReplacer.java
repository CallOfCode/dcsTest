package com.baiyyy.didcs.module.replacer;

import com.baiyyy.didcs.common.constant.ReplaceConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.interfaces.replacer.IReplacer;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 通用替换程序
 *
 * @author 逄林
 */
public class CommonReplacer implements IReplacer {
    private String[] filedArr = null;
    private String replaceStrategy = null;

    @Override
    public void initParam(String fields, String replaceStrategy) {
        if(StringUtils.isNotBlank(fields)){
            filedArr = fields.split(",");
        }
        this.replaceStrategy = replaceStrategy;
    }

    @Override
    public List doReplace(Map dataRow, Map stdRow) {
        List retList = new ArrayList();

        if(null!=filedArr){
            for(int i=0;i<filedArr.length;i++){
                if(ReplaceConstant.REPLACE_NULL.equals(replaceStrategy)){
                    //非空替换
                    if(dataRow.containsKey(filedArr[i]+"_stdid")){
                        //判断id，当源值id非空且标准值id为空时进行替换
                        if(MapUtil.isNotBlank(MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdid"))) && MapUtil.isBlank(MapUtil.getCasedString(stdRow.get(filedArr[i]+"_id")))){
                            retList.add(filedArr[i]);
                        }
                    }else if(dataRow.containsKey(filedArr[i]+"_stdstr")){
                        //判断str，当源值非空且标准值为空时进行替换
                        if(MapUtil.isNotBlank(MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdstr"))) && MapUtil.isBlank(MapUtil.getCasedString(stdRow.get(filedArr[i])))){
                            retList.add(filedArr[i]);
                        }
                    }
                }else if(ReplaceConstant.REPLACE_FORCE.equals(replaceStrategy)){
                    //强制替换
                    if(dataRow.containsKey(filedArr[i]+"_stdid")){
                        //判断id，当源值id非空且不等于标准值id时进行替换
                        if(MapUtil.isNotBlank(MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdid"))) && !MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdid")).equals(MapUtil.getCasedString(stdRow.get(filedArr[i]+"_id")))){
                            retList.add(filedArr[i]);
                        }
                    }else if(dataRow.containsKey(filedArr[i]+"_stdstr")){
                        //判断str，当源值非空且不等于标准值进行替换
                        if(MapUtil.isNotBlank(MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdstr"))) && !MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdstr")).equals(MapUtil.getCasedString(stdRow.get(filedArr[i])))){
                            retList.add(filedArr[i]);
                        }
                    }
                }else if(ReplaceConstant.REPLACE_NOTIFY.equals(replaceStrategy)){
                    //通知替换
                    if(dataRow.containsKey(filedArr[i]+"_stdid")){
                        //判断id，当源值id非空且不等于标准值id时进行通知
                        if(MapUtil.isNotBlank(MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdid"))) && !MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdid")).equals(MapUtil.getCasedString(stdRow.get(filedArr[i]+"_id")))){
                            retList.add(filedArr[i]);
                        }
                    }else if(dataRow.containsKey(filedArr[i]+"_stdstr")){
                        //判断str，当源值非空且不等于标准值进行通知
                        if(MapUtil.isNotBlank(MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdstr"))) && !MapUtil.getCasedString(dataRow.get(filedArr[i]+"_stdstr")).equals(MapUtil.getCasedString(stdRow.get(filedArr[i])))){
                            retList.add(filedArr[i]);
                        }
                    }
                }
            }
        }

        return retList;
    }

    @Override
    public String getReplaceStrategy(){
        return this.replaceStrategy;
    }
}
