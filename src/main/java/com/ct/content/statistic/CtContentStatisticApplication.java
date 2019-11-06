package com.ct.content.statistic;

import org.springframework.boot.SpringApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.zeasn.common.bootstrap.MySpringBootApplication;
import com.zeasn.common.feign.MyEnableFeignClients;

import tk.mybatis.spring.annotation.MapperScan;

@MySpringBootApplication(scanBasePackages = {"com.ct.content.statistic", "com.zeasn.common.component.global"})
@EnableEurekaClient
@MyEnableFeignClients
@EnableTransactionManagement(proxyTargetClass = true)
@MapperScan(basePackages = {"com.ct.**.mapper"})
public class CtContentStatisticApplication {

	public static void main(String[] args) {
		SpringApplication.run(CtContentStatisticApplication.class, args);
	}

}

