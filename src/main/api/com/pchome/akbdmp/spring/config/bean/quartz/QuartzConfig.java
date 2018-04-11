package com.pchome.akbdmp.spring.config.bean.quartz;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.api.call.ad.controller.AdClassSendKafkaMapJob;

@Component
@EnableScheduling
public class QuartzConfig implements SchedulingConfigurer {

	final static Logger log = Logger.getLogger(QuartzConfig.class);

//	@Bean
//	public AdClassSendKafkaMapJob bean() {
//		return new AdClassSendKafkaMapJob();
//	}

	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.setScheduler(taskExecutor());
	}

	@Bean(destroyMethod = "shutdown")
	public Executor taskExecutor() {
		return Executors.newScheduledThreadPool(10);
	}
}
