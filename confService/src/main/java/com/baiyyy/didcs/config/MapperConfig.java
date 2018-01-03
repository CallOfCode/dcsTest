package com.baiyyy.didcs.config;

import org.mybatis.spring.mapper.MapperScannerConfigurer;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Mapper 配置
 * 定义Mapper的扫描路径
 *
 * @author 逄林
 */
@Configuration
@AutoConfigureAfter(MybatisConfig.class)
public class MapperConfig implements EnvironmentAware {
    private RelaxedPropertyResolver propertyResolver;

    private String basePackage;
    @Bean
    public MapperScannerConfigurer mapperScannerConfigurer(Environment environment){
        MapperScannerConfigurer mapperScannerConfigurer = new MapperScannerConfigurer();
        mapperScannerConfigurer.setSqlSessionFactoryBeanName("sqlSessionFactory");
        mapperScannerConfigurer.setBasePackage(basePackage);
        return mapperScannerConfigurer;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.propertyResolver = new RelaxedPropertyResolver(environment, null);
        this.basePackage = propertyResolver.getProperty("mybatis.basepackage");
    }
}
