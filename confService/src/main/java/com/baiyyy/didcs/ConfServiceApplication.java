package com.baiyyy.didcs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableTransactionManagement
@ServletComponentScan("com.baiyyy.didcs")
public class ConfServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConfServiceApplication.class, args);
	}
}
