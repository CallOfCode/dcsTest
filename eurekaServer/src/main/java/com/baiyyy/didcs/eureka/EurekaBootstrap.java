package com.baiyyy.didcs.eureka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * EurekaServer
 *
 * @author 逄林
 */
@SpringBootApplication
@EnableEurekaServer
public class EurekaBootstrap {

    public static void main(String[] args){
        SpringApplication.run(EurekaBootstrap.class, args);
    }

}
