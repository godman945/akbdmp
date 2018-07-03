//package com.pchome.akbdmp.adm.job.quartz;
//
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.HashMap;
//
//import org.apache.log4j.Logger;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.scheduling.annotation.Scheduled;
//
//import com.pchome.akbdmp.api.data.enumeration.DmpLogKeyEnum;
//import com.pchome.akbdmp.data.mysql.pojo.KdclStatisticsSource;
//import com.pchome.akbdmp.mysql.db.service.dmp.IKdclStatisticsSourceService;
//
//public class KdclStatisticsSourceJob {
//	Logger log = Logger.getLogger(KdclStatisticsSourceJob.class);
// 	
//	@Autowired
//	IKdclStatisticsSourceService kdclStatisticsSourceService; 
//	
//	@Autowired
//	RedisTemplate<String, Object> redisTemplate;
//	
//	@Value("${dmp.radis.key}")
//	private String[] dmpRadisKey;
//	
//	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//	
////	@Scheduled(cron="0 0 4 * * *")
////	@Scheduled(fixedDelay = 180000)
//    public void execute() {
//		try{
//			log.info("****************** WRITE HADOOP DMP JOB START ******************");
//			Calendar calendar = Calendar.getInstance();
////			calendar.add(Calendar.DAY_OF_MONTH, -1);
//			String radisRecodeDate = sdf.format(calendar.getTime());
//			
//			radisRecodeDate = "2018-05-22";
//			
//			kdclStatisticsSourceService.deleteDmpCountByDate(radisRecodeDate);
//			Date date = new Date();
//			
//			for (String key : dmpRadisKey) {
//				radisRecodeDate = "2018-07-03";
//				String radisKey = key.replace("[DAY]", radisRecodeDate);
//				for (DmpLogKeyEnum dmpLogKey : DmpLogKeyEnum.values()) {
//					if(radisKey.contains(dmpLogKey.getKey())){
//						String idType = dmpLogKey.getIdType();
//						String serviceType = dmpLogKey.getServiceType();
//						String behavior = dmpLogKey.getBehavior();
//						String classify = dmpLogKey.getClassify();
//						Integer count =  (Integer) redisTemplate.opsForValue().get(radisKey);
//						if(count == null){
//							count = 0;
//						}
//						System.out.println(radisKey+" count:"+count);
//						
//						radisRecodeDate = "2018-05-22";
//						saveDmpLog(count,idType,serviceType,behavior,classify,radisRecodeDate,date);
////						redisTemplate.delete(radisKey);
//						break;
//					}
//				}
//			}
//			
//			
//			
//			
//			
////			for (String key : dmpRadisKey) {
////				String radisKey = key.replace("[DAY]", radisRecodeDate);
////				for (DmpLogKeyEnum dmpLogKey : DmpLogKeyEnum.values()) {
////					if(radisKey.contains(dmpLogKey.getKey())){
////						
////						radisRecodeDate = "2018-07-03";
////						
////						String idType = dmpLogKey.getIdType();
////						String serviceType = dmpLogKey.getServiceType();
////						String behavior = dmpLogKey.getBehavior();
////						String classify = dmpLogKey.getClassify();
////						int count =  redisTemplate.opsForValue().get(radisKey) == null ? 0 : (int)redisTemplate.opsForValue().get(radisKey);
////						System.out.println(radisKey+" count:"+count);
////						
////						saveDmpLog(count,idType,serviceType,behavior,classify,radisRecodeDate,date);
////						
////						radisRecodeDate ="2018-05-22";
////						
////						break;
////					}
////				}
////			}
//		}catch(Exception e){
//			log.error(e.getMessage());
//			log.error("****************** FAIL WRITE HADOOP DMP JOB END ******************");
//		}
//		log.info("****************** WRITE HADOOP DMP JOB START END ******************");
//	}
//	
//	private void saveDmpLog(int count,String idType,String serviceType,String behavior,String classify,String radisRecodeDate,Date date) throws Exception{
//		KdclStatisticsSource KdclStatisticsSource = new KdclStatisticsSource();
//		KdclStatisticsSource.setIdType(idType);
//		KdclStatisticsSource.setServiceType(serviceType);
//		KdclStatisticsSource.setBehavior(behavior);
//		KdclStatisticsSource.setClassify(classify);
//		KdclStatisticsSource.setCounter(count);
//		KdclStatisticsSource.setRecordDate(radisRecodeDate);
//		KdclStatisticsSource.setUpdateDate(date);
//		KdclStatisticsSource.setCreateDate(date);
//		kdclStatisticsSourceService.save(KdclStatisticsSource);
//	}
//}
//
