package com.baiyyy.didcs.controller.mng;

import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.service.elsearch.EsCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * 负责进行Es缓存的初始化
 *
 * @author 逄林
 */
@RestController
public class EsCacheController {
    @Autowired
    private EsCacheService esCacheService;

    /**
     * 根据名称初始化执行缓存
     * @param name
     * @return
     */
    @RequestMapping(value="/escache/init/{name}",method= RequestMethod.GET)
    public JsonResult initEsCacheByName(@PathVariable(value = "name") String name){
        return esCacheService.initEsCacheByName(name);
    }

    /**
     * 根据名称刷新缓存
     * @param name
     * @return
     */
    @RequestMapping(value="/escache/refresh/{name}",method= RequestMethod.GET)
    public JsonResult refreshEsCacheByName(@PathVariable(value = "name") String name){
        return esCacheService.refreshEsCacheByName(name);
    }

    /**
     * 根据名称创建缓存
     * @param index
     * @return
     */
    @RequestMapping(value="/escache/create/{index}",method= RequestMethod.GET)
    public JsonResult initSpEsCacheByName(@PathVariable(value = "index") String index){
        return esCacheService.creanteSysEsCacheByIndex(index);
    }

}
