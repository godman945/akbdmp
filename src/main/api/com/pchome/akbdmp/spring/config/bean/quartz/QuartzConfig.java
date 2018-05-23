package com.pchome.akbdmp.spring.config.bean.quartz;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.adm.job.quartz.KdclStatisticsSourceJob;
import com.pchome.akbdmp.adm.job.quartz.MySqlTestJob;

@Component
@EnableScheduling
public class QuartzConfig implements SchedulingConfigurer {

	final static Logger log = Logger.getLogger(QuartzConfig.class);

	@Bean
	public KdclStatisticsSourceJob bean() {
		return new KdclStatisticsSourceJob();
	}

	@Bean
	public MySqlTestJob execute() {
		return new MySqlTestJob();
	}
	
	
	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		taskRegistrar.setScheduler(taskExecutor());
	}

	@Bean(destroyMethod = "shutdown")
	public Executor taskExecutor() {
		return Executors.newScheduledThreadPool(10);
	}
}
