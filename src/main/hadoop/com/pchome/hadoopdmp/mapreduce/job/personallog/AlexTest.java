package com.pchome.hadoopdmp.mapreduce.job.personallog;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.KdclStatisticsSourceService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

public class AlexTest {

	
	
	
	
	
	public static void main(String[] args) {
		System.setProperty("spring.profiles.active", "prd");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		KdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
		
		System.out.println(kdclStatisticsSourceService.loadAll().size());
		
	}

}
