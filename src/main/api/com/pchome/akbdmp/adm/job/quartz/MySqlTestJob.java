package com.pchome.akbdmp.adm.job.quartz;

import org.mortbay.log.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import com.pchome.akbdmp.mysql.db.service.dmp.IKdclStatisticsSourceService;

public class MySqlTestJob {
	@Autowired
	IKdclStatisticsSourceService kdclStatisticsSourceService; 
	
	@Scheduled(fixedDelay = 180000)
	public void execute() {
		Log.info("kdclStatisticsSource size:"+kdclStatisticsSourceService.loadAll().size());
	}
}
