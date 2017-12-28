package com.baiyyy.didcs.common.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * SpringContext工具类
 *
 * @author 逄林
 */
@Component
public class SpringContextUtil implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    public static ApplicationContext getApplicationContext() {
        return SpringContextUtil.applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (null == SpringContextUtil.applicationContext) {
            SpringContextUtil.applicationContext = applicationContext;
        }
    }

    /**
     * 使用此方法获取spring中注册的bean
     *
     * @param name
     * @return
     * @throws BeansException
     */
    public static <T> T getBean(String name) throws BeansException {
        return (T) getApplicationContext().getBean(name);
    }

}
