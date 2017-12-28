package com.baiyyy.didcs.service.invoker;

import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.listener.SpringPropertyListener;
import com.baiyyy.didcs.common.util.JoddHttpUtil;
import com.baiyyy.didcs.interfaces.invoker.IStageServiceInvoker;
import com.baiyyy.didcs.service.dispatcher.LockService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static net.logstash.logback.marker.Markers.append;


/**
 * 批量格式化服务调用程序
 * 在模块中进行线程拆分及调用
 *
 * @author 逄林
 */
@Service
public class BatchStdServiceInvoker extends AbstractStageServiceInvoker implements IStageServiceInvoker {
    Logger logger = LoggerFactory.getLogger(BatchStdServiceInvoker.class);
    @Autowired
    LockService lockService;

    @Override
    public String getInvokerName() {
        return "批量标准化";
    }

    @Override
    public Integer initThreadNums() {
        return 1;
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
}
