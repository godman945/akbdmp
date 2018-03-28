package com.pchome.akbdmp.api.call.ad.controller;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

public class JobTest {
	Logger log = Logger.getLogger(JobTest.class);
 	
	@Autowired
 	private HashMap<String,String> sendKafkaMap;
 	
 	
	//@Scheduled(cron="1 * * * * *")
	@Scheduled(fixedDelay = 15000)
    public void execute() {
		try{
			log.info("****************** call kafka job start ******************");
			
			log.info(">>>>>>>>>>>>>>>sendKafkaMap size:"+sendKafkaMap.size());
			
			
			
			
			
//			List<PfpAdVideoSource> pfpAdVideoSourceList = pfpAdVideoSourceService.findNeedDownloadVideo();
//			
//			log.info("pfpAdVideoSourceList:"+pfpAdVideoSourceList.size());
//			
//			for (PfpAdVideoSource pfpAdVideoSource : pfpAdVideoSourceList) {
//				Map<String,List<VideoDownloadVO>> videoDetailInfoMap = pfpAdDetailService.findAdadDetailVideoInfo(pfpAdVideoSource.getAdVideoUrl());
//				if(videoDetailInfoMap.size() > 0){
//					pfpAdVideoSource.setAdVideoMp4Path("開始下載");
//					pfpAdVideoSource.setAdVideoWebmPath("開始下載");
//					pfpAdVideoSource.setAdVideoStatus(EnumAdVideoDownloadStatus.DOWNLOAD.getStatus());
//					pfpAdVideoSource.setUpdateDate(date);
//					pfpAdVideoSourceService.saveOrUpdate(pfpAdVideoSource);
//					
//					Map<String,PfpAdDetail> pfpAdDetailMap = changeVideoDownloadStatus(videoDetailInfoMap);
//					VideoDownloadResultVO videoDownloadResultVO = processDownloadVideo(pfpAdVideoSource.getAdVideoUrl(),pfpAdVideoSource.getAdVideoSeq());
//					changeVideoDownloadFinishStatus(pfpAdDetailMap,videoDownloadResultVO,pfpAdVideoSource);
//				}
//			}
		}catch(Exception e){
			e.printStackTrace();
			log.info("****************** FAIL DOWNLOAD VIDEO END ******************");
		}
		log.info("****************** call kafka job END ******************");
	}
	
}

