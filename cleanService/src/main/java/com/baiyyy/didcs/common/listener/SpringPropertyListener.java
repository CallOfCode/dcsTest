package com.baiyyy.didcs.common.listener;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.stereotype.Component;
import org.springframework.util.StringValueResolver;

/**
 * Spring属性加载监听器
 * @fileName SpringPropertyListener.java
 * @description
 *
 * @author 逄林
 */
@Component
public class SpringPropertyListener implements InitializingBean, EmbeddedValueResolverAware {

	public static StringValueResolver stringValueResolver;
	
	@Override
	public void setEmbeddedValueResolver(StringValueResolver resolver) {
		SpringPropertyListener.stringValueResolver = resolver;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
	}

	/**
	 * 根据属性名获取属性值
	 * @param name
	 * @return
	 */
	public static String getPropertyValue(String name){
		return stringValueResolver.resolveStringValue(name);
	}
}
