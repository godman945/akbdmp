package com.pchome.akbdmp.api.call.ad.controller;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

public class AdClassSendKafkaMapJob {
	Logger log = Logger.getLogger(AdClassSendKafkaMapJob.class);
 	
	@Autowired
 	private HashMap<String,Object> sendKafkaMap;
 	
 	
	@Scheduled(cron="0 0 2 * * *")
//	@Scheduled(fixedDelay = 10000)
    public void execute() {
		try{
			log.info("****************** CLEAN MAP FOR DMP KAFKA JOB START ******************");
			log.info("before sendKafkaMap size:"+sendKafkaMap.size());
			sendKafkaMap.clear();
			log.info("after sendKafkaMap size:"+sendKafkaMap.size());
		}catch(Exception e){
			e.printStackTrace();
			log.info("****************** FAIL DOWNLOAD VIDEO END ******************");
		}
		log.info("****************** CLEAN MAP FOR DMP KAFKA JOB START END ******************");
	}
	
}

