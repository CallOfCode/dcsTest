package com.baiyyy.didcs.common.listener;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

@Component
public class SpringDisposableBean implements DisposableBean,ExitCodeGenerator {
    @Override
    public void destroy() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.stop();
    }

    @Override
    public int getExitCode() {
        return 5;
    }
}
