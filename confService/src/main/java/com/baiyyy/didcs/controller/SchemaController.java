package com.baiyyy.didcs.controller;

import com.baiyyy.didcs.common.pojo.LayTableBean;
import com.baiyyy.didcs.service.SchemaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/schemas")
public class SchemaController {
    @Autowired
    private SchemaService schemaService;

    @RequestMapping("")
    public String toList(){
        return "schema/list";
    }

    @RequestMapping("/list")
    @ResponseBody
    public LayTableBean getSchemasPage(@RequestParam String name,@RequestParam Integer page,@RequestParam Integer limit){
        int offset = (page-1)*limit;
        List<Map> list = schemaService.getPageSchemas(name,limit,offset);
        if(null==list){
            list = new ArrayList();
        }

        LayTableBean table = new LayTableBean(0,"",1000, list);
        return table;
    }

}
