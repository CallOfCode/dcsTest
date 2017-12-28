package com.baiyyy.didcs.controller.sv;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.module.invoker.CommonFlowInvoker;
import com.baiyyy.didcs.service.dispatcher.LockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.append;
import static net.logstash.logback.marker.Markers.appendEntries;

/**
 * 通用服务调用模块，模块由StageServiceInvoker调用
 * 该模块将执行一个由StageServiceInvoker分解产生的独立线程
 * 在调用过程中，依据传入的service参数进行流程与节点的封装，并执行流程
 * 如果在封装阶段出现异常，则会将当前流程记为结束
 *
 * @author 逄林
 */
@RestController
@RequestMapping(value = "/serviceFlow")
public class CommonSvInvokerController {
    Logger logger = LoggerFactory.getLogger(CommonSvInvokerController.class);
    @Autowired
    LockService lockService;

    @RequestMapping(value = "/invoke", method = RequestMethod.POST)
    public void doInvoke(@RequestParam("serviceStr") String serviceStr, @RequestParam("schemaId") String schemaId, @RequestParam("taskId") String taskId, @RequestParam("batchId") String batchId, @RequestParam("maxId") String maxId, @RequestParam("minId") String minId, @RequestParam("limitIds") String limitIds, @RequestParam("handleSize") Integer handleSize, @RequestParam("lockPath") String lockPath, @RequestParam("userId") String userId) {
        JSONObject service = JSONObject.parseObject(serviceStr);
        JSONArray nodes = service.getJSONArray("nodes");
        try {
            ICleanFlow flow = (ICleanFlow) Class.forName(service.getString("flow")).newInstance();
            flow.init(schemaId, taskId, batchId, userId, maxId, minId, limitIds, handleSize, lockPath);
            List<ICleanNode> nodesList = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                nodesList.add((ICleanNode) Class.forName(nodes.getString(i)).newInstance());
            }
            CommonFlowInvoker flowInvoker = new CommonFlowInvoker(flow, nodesList);
            flowInvoker.doInvoke();
        } catch (Exception e) {
            Map sourceMap = new HashMap();
            sourceMap.put("serviceStr",serviceStr);
            sourceMap.put("batchId",batchId);
            sourceMap.put("maxId",maxId);
            sourceMap.put("minId",minId);
            sourceMap.put("limitIds",limitIds);
            sourceMap.put("lockPath",lockPath);
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_FLOW).and(appendEntries(sourceMap)),LogConstant.LGS_FLOW_ERRORMSG_INVOKER,e );
            //如果调用出错则当前线程结束并计数器-1
            lockService.subLockNum(lockPath, batchId, userId);
        }
    }

}
