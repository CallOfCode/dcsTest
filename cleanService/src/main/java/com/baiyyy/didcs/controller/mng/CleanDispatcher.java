package com.baiyyy.didcs.controller.mng;

import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.service.dispatcher.CleanDispatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import static net.logstash.logback.marker.Markers.append;

/**
 * 清洗调度器，负责节点的启动、终止、结束的调度
 *
 * @author 逄林
 */
@RestController
public class CleanDispatcher {
    Logger logger = LoggerFactory.getLogger(CleanDispatcher.class);
    @Autowired
    private CleanDispatcherService cleanDispatcherService;

    /**
     * 接收参数，执行流程调度
     */
    @RequestMapping(value = "/dispatch/{batchId}",method = RequestMethod.GET)
    public JsonResult<String> doDispatch(@PathVariable(value = "batchId") String batchId){
        JsonResult<String> r = null;
        //配置校验，判断是否存在相应版本的配置
        if(cleanDispatcherService.checkAndInitConf(batchId)){
            logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_SUCCMSG_SVINVOKE);
            r = cleanDispatcherService.dispatch(batchId,null);
        }else{
            logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_ERRORMSG_SVINVOKE_FAILED);
            r = new JsonResult<>();
            r.setResult(false);
            r.setMsg("版本校验出错");
        }
        return r;
    }



}
