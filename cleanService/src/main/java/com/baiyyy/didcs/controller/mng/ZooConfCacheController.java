package com.baiyyy.didcs.controller.mng;

import com.baiyyy.didcs.common.constant.ZooCacheConstant;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.service.dispatcher.ZooConfCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * zookeeper cache服务
 * 用于管理配置缓存
 *
 * @author 逄林
 */
@RestController
@RequestMapping("/zooConfCache")
public class ZooConfCacheController {

    @Autowired
    ZooConfCacheService zooConfCacheService;

    /**
     * 初始化配置缓存，包括元数据和任务级别的配置缓存
     * @return
     */
    @RequestMapping(value = "/initAllConf",method = RequestMethod.GET)
    public JsonResult<String> initAllFlowConf(){
        JsonResult<String> r = zooConfCacheService.initAllConf();
        return r;
    }

    /**
     * 根据流程配置，实例化批次配置
     * @param batchId
     * @return
     */
    @RequestMapping(value = "/initBatchConf/{batchId}",method = RequestMethod.GET)
    public JsonResult<String> initBatchConf(@PathVariable(value = "batchId") String batchId){
        JsonResult<String> r = zooConfCacheService.initConfByBatchId(batchId);
        return r;
    }

    /**
     * 清除已经实例化的清洗配置
     * @param batchId
     * @return
     */
    @RequestMapping(value = "/removeBatchConf/{batchId}",method = RequestMethod.GET)
    public JsonResult<String> removeBatchConf(@PathVariable(value = "batchId") String batchId){
        JsonResult<String> r = zooConfCacheService.removeBatchConf(batchId);
        return r;
    }

    /**
     * 根据批次id获取清洗配置，包括schema 配置，task配置以及已经实例化正在运行的配置
     * @param confType 配置类型：run schema task
     * @param batchId
     * @return
     */
    @RequestMapping(value = "/batchConf/{confType}/{batchId}",method = RequestMethod.GET)
    public JsonResult<String> getBatchRunConf(@PathVariable(value = "confType") String confType, @PathVariable(value = "batchId") String batchId){
        String confPath = null;
        if("run".equals(confType.toLowerCase())){
            confPath = ZooCacheConstant.BATCH_PATH+"/"+batchId;
        }else if("schema".equals(confType.toLowerCase())){
            confPath = ZooCacheConstant.SCHEMA_PATH+"/"+batchId;
        }else{
            confPath = ZooCacheConstant.TASK_PATH+"/"+batchId;
        }

        JsonResult<String> r = zooConfCacheService.getBatchConf(batchId,confPath);
        return r;
    }


}
