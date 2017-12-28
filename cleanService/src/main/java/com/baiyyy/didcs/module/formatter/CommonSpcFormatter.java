package com.baiyyy.didcs.module.formatter;

import com.baiyyy.didcs.common.constant.FormatterContstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.interfaces.formatter.IFormatter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通用特殊字符格式化
 * 用于处理单字符
 *
 * @author 逄林
 */
public class CommonSpcFormatter implements IFormatter{
    private List<Map> rules;
    private Map<String,String> patternMap = new HashMap<>();
    private static final String REGEX = "\\\\|\\.|\\*|\\^|\\$|\\(|\\)|\\[|\\]|\\{|\\}|\\?|\\+|\\:|\\,|\\=|\\||\\-";

    @Override
    public void initRules(List rules) {
        this.rules = rules;
        //将相同模式的特殊字符合并到一个规则中
        if(null!=this.rules){
            for(Map rule:this.rules){
                String chars = MapUtil.getCasedString(rule.get("chars"));
                String pos = MapUtil.getCasedString(rule.get("position"));
                if(StringUtils.isNoneBlank(chars,pos)){
                    if(patternMap.containsKey(pos)){
                        patternMap.put(pos,patternMap.get(pos)+"|"+parseChars(chars));
                    }else{
                        patternMap.put(pos,parseChars(chars));
                    }
                }
            }
        }

    }

    @Override
    public Object doFormat(Object element) {
        String str = element.toString();
        return processEnd(processEnd(processMiddle(processContain(str))));
    }

    /**
     * 处理以某特殊字符开头的
     * @param str
     * @return
     */
    private String processBegin(String str){
        try{
            if(StringUtils.isNoneBlank(str)&&patternMap.containsKey(FormatterContstant.FMT_SPC_BEGIN)){
                Pattern  pattern = Pattern.compile(patternMap.get(FormatterContstant.FMT_SPC_BEGIN));
                Matcher matcher = pattern.matcher(str);
                while(matcher.find()){
                    if(matcher.start()==0){
                        str = matcher.replaceFirst("");
                        matcher = pattern.matcher(str);
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return str;
    }

    /**
     * 处理以某特殊字符结尾的
     * @param str
     * @return
     */
    private String processEnd(String str){
        try{
            if(StringUtils.isNoneBlank(str)&&patternMap.containsKey(FormatterContstant.FMT_SPC_END)){
                Pattern  pattern = Pattern.compile(patternMap.get(FormatterContstant.FMT_SPC_END));
                Matcher matcher = pattern.matcher(str);
                while(matcher.find()){
                    if(matcher.end()==str.length()){
                        str = str.substring(0, str.length()-1);
                        matcher = pattern.matcher(str);
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return str;
    }

    /**
     * 处理包含某特殊字符的
     * @param str
     * @return
     */
    private String processContain(String str){
        try{
            if(StringUtils.isNoneBlank(str)&&patternMap.containsKey(FormatterContstant.FMT_SPC_CONTAIN)){
                Pattern  pattern = Pattern.compile(patternMap.get(FormatterContstant.FMT_SPC_CONTAIN));
                Matcher matcher = pattern.matcher(str);
                if(matcher.find()){
                    str = matcher.replaceAll("");
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return str;
    }

    /**
     * 处理包含某特殊字符的,首位除外
     * @param str
     * @return
     */
    private String processMiddle(String str){
        try{
            if(StringUtils.isNoneBlank(str)&&patternMap.containsKey(FormatterContstant.FMT_SPC_MIDDLE)){
                Pattern  pattern = Pattern.compile(patternMap.get(FormatterContstant.FMT_SPC_MIDDLE));
                Matcher matcher = pattern.matcher(str.substring(1,str.length()-1));
                if(matcher.find()){
                    str = str.substring(0,1)+matcher.replaceAll("")+str.substring(str.length()-1);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return str;
    }

    /**
     * 对于正则表达式中的特殊字符进行转义
     * @param chars
     * @return
     */
    private String parseChars(String chars){
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(chars);

        if(matcher.find()){
            return "\\"+chars;
        }else{
            return chars;
        }
    }
}
