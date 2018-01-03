package com.baiyyy.didcs.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequestMapping("/schemas")
public class SchemaController {

    @RequestMapping("")
    public String toList(){
        return "schema/list";
    }

    @RequestMapping("/list")
    @ResponseBody
    public List getSchemas(){
        return null;
    }

}
