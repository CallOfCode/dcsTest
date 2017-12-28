package com.baiyyy.didcs.service.invoker;

import com.baiyyy.didcs.common.constant.SpringCloudConstant;
import com.baiyyy.didcs.interfaces.invoker.IStageServiceInvoker;
import com.baiyyy.didcs.service.dispatcher.LockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 组内查重service调用程序
 *
 * @author 逄林
 */
@Service
public class InnerMatchServiceInvoker extends AbstractStageServiceInvoker implements IStageServiceInvoker {
    private Logger logger = LoggerFactory.getLogger(InnerMatchServiceInvoker.class);
    @Autowired
    LockService lockService;

    @Override
    public String getInvokerName() {
        return "数据查重";
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
        return null;
    }

    @Override
    public Integer initThreadNums() {
        return SpringCloudConstant.DEFAULT_THREADS_NUM;
    }
}
