package com.baiyyy.didcs.controller.mng;

import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.service.dispatcher.SchemaTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import static net.logstash.logback.marker.Markers.append;

/**
 * 元数据相关服务接口
 *
 * @author 逄林
 */
@RestController
@RequestMapping("/schema")
public class SchemaController {
    private Logger logger = LoggerFactory.getLogger(SchemaController.class);
    @Autowired
    private SchemaTaskService schemaTaskService;

    /**
     * 根据taskId创建表
     * @param taskId
     * @return
     */
    @RequestMapping(value = "/createTable/{taskId}",method = RequestMethod.GET)
    public JsonResult<String> createTable(@PathVariable(value = "taskId") String taskId){
        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_SCHEMA).and(append("taskId",taskId)),LogConstant.LGS_SCHEMA_SUCCMSG_CRTABLE_BEGIN );
        JsonResult<String> r = schemaTaskService.createTableForTask(taskId);
        return r;
    }

}
