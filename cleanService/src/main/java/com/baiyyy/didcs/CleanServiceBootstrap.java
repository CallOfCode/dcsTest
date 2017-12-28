package com.baiyyy.didcs;

import com.baiyyy.didcs.common.util.SpringContextUtil;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * 服务启动类
 *
 * @author 逄林
 */
@SpringBootApplication
@EnableEurekaClient
@EnableTransactionManagement
@ServletComponentScan("com.baiyyy.didcs")
public class CleanServiceBootstrap {

    public static void main(String[] args) {
        new SpringApplicationBuilder(CleanServiceBootstrap.class).web(true).run();
    }

}
