package com.baiyyy.didcs.zuul;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.netflix.zuul.EnableZuulProxy;

/**
 * zuul 服务
 *
 * @author 逄林
 */
@SpringBootApplication
@EnableEurekaClient
@EnableZuulProxy
public class ZuulBootstrap {

    public static void main(String[] args){
        new SpringApplicationBuilder(ZuulBootstrap.class).web(true).run(args);
    }

}
