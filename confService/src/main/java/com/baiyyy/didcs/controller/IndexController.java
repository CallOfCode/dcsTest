package com.baiyyy.didcs.controller;

import com.baiyyy.didcs.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/index")
public class IndexController {
    @Autowired
    private IndexService indexService;

    @RequestMapping("")
    public String toIndex(Model model){
        return "index";
    }

    @RequestMapping("/main")
    public String toMain(Model model){
        model.addAttribute("schemaCount",indexService.getSchemaCount());
        model.addAttribute("taskCount",indexService.getTaskCount());
        model.addAttribute("batchCount",indexService.getBatchCount());
        model.addAttribute("runBatchCount",indexService.getRunBatchCount());
        return "main";
    }

}
