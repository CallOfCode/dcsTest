package com.baiyyy.didcs.module.formatter;

import com.baiyyy.didcs.interfaces.matcher.IMatcher;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FormatterTest {



    public static void main( String args[] ){
//        testStart();
//        testEnd();
        testContain();
//        testBetween();
//        testSpchars();
    }

    public static void testStart(){
        String regex = "!|\\*|\b";
        String input = "!!*!*!abcoo!ooooooooooooo";

        Pattern  pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        while(matcher.find()){
            if(matcher.start()==0){
                input = matcher.replaceFirst("");
                matcher = pattern.matcher(input);
            }
        }
        System.out.println(input);
    }

    public static void testEnd(){
        String regex = "!|\\*|\b";
        String input = "xyzoooooooabcooooooooab!c*!";

        Pattern  pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        while(matcher.find()){
            if(matcher.end()==input.length()){
                input = input.substring(0, input.length()-1);
                matcher = pattern.matcher(input);
            }
        }
        System.out.println(input);
    }

    public static void testContain(){



        String regex = "!|\\*";
        String input = "^逄林*";

        Pattern  pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        if(matcher.find()){
            input = matcher.replaceAll("");
        }
        System.out.println(input);
    }

    public static void testBetween(){
        String regex = "!|\\*|\b|\\(|\\)";
        String input = "xyzoooooooa(bc)ooooooooab!c*!";

        Pattern  pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input.substring(1,input.length()-1));

        if(matcher.find()){
            input = input.substring(0,1)+matcher.replaceAll("")+input.substring(input.length()-1);
        }
        System.out.println(input);
    }

    public static void testSpchars(){

        String regex = "\\\\|\\.|\\*|\\^|\\$|\\(|\\)|\\[|\\]|\\{|\\}|\\?|\\+|\\:|\\,|\\=|\\||\\-";
        String input = "\\";

        Pattern  pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        if(matcher.find()){
            System.out.println("\\\\"+input);
        }else{
            System.out.println(input);
        }

    }

}
