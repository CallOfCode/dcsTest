package com.baiyyy.didcs.service.invoker;

import com.baiyyy.didcs.common.constant.SpringCloudConstant;
import com.baiyyy.didcs.interfaces.invoker.IStageServiceInvoker;
import com.baiyyy.didcs.service.dispatcher.LockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 补全服务调用程序
 *
 * @author 逄林
 */
@Service
public class RepairServiceInvoker extends AbstractStageServiceInvoker implements IStageServiceInvoker {
    private Logger logger = LoggerFactory.getLogger(RepairServiceInvoker.class);
    @Autowired
    DiscoveryClient discoveryClient;
    @Autowired
    LockService lockService;

    @Override
    public String getInvokerName() {
        return "数据补全";
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public LockService getLockService() {
        return lockService;
    }

    @Override
    public List<Map<String, Object>> getHeadIds(int threads, Map<String, Object> param) {
        //TODO 补全线程分配逻辑
        List<Map<String ,Object>> list = new ArrayList<>(threads);
        if(threads>1){
            for(int i=0;i<threads;i++){
                Map tmpIdMap = new HashMap();
                tmpIdMap.put("maxId","0");
                tmpIdMap.put("minId","0");
                list.add(i,tmpIdMap);
            }
        }
        return list;
    }

    @Override
    public Integer initThreadNums() {
        List<ServiceInstance> list = discoveryClient.getInstances(SpringCloudConstant.CLEAN_SERVICE_NAME);
        if(null!=list&&list.size()>0){
            return list.size();
        }else{
            return SpringCloudConstant.DEFAULT_THREADS_NUM;
        }
    }
}
