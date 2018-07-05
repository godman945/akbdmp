package com.pchome.akbdmp.cron.job;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.api.data.enumeration.DmpLogKeyEnum;
import com.pchome.akbdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.akbdmp.mysql.db.service.dmp.IKdclStatisticsSourceService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

@Component
public class KdclStatisticsSourceJob {
	Logger log = Logger.getLogger(KdclStatisticsSourceJob.class);
	
	@Autowired
	IKdclStatisticsSourceService kdclStatisticsSourceService; 
	
	@Autowired
	RedisTemplate<String, Object> redisTemplate;
	
	@Value("${dmp.radis.key}")
	private String[] dmpRadisKey;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public void excute(){
		try{
			log.info("****************** WRITE HADOOP DMP JOB START ******************");
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			String radisRecodeDate = sdf.format(calendar.getTime());
			kdclStatisticsSourceService.deleteDmpCountByDate(radisRecodeDate);
			Date date = new Date();
			for (String key : dmpRadisKey) {
				String radisKey = key.replace("[DAY]", radisRecodeDate);
				for (DmpLogKeyEnum dmpLogKey : DmpLogKeyEnum.values()) {
					if(radisKey.contains(dmpLogKey.getKey())){
						String idType = dmpLogKey.getIdType();
						String serviceType = dmpLogKey.getServiceType();
						String behavior = dmpLogKey.getBehavior();
						String classify = dmpLogKey.getClassify();
						Integer count =  (Integer) redisTemplate.opsForValue().get(radisKey);
						if(count == null){
							count = 0;
						}
						log.info(radisKey+" count:"+count);
						saveDmpLog(count,idType,serviceType,behavior,classify,radisRecodeDate,date);
						break;
					}
				}
			}
		}catch(Exception e){
			log.error(e.getMessage());
			log.error("****************** FAIL WRITE HADOOP DMP JOB END ******************");
		}
		log.info("****************** WRITE HADOOP DMP JOB START END ******************");
	}
	
	private void saveDmpLog(int count,String idType,String serviceType,String behavior,String classify,String radisRecodeDate,Date date) throws Exception{
		KdclStatisticsSource KdclStatisticsSource = new KdclStatisticsSource();
		KdclStatisticsSource.setIdType(idType);
		KdclStatisticsSource.setServiceType(serviceType);
		KdclStatisticsSource.setBehavior(behavior);
		KdclStatisticsSource.setClassify(classify);
		KdclStatisticsSource.setCounter(count);
		KdclStatisticsSource.setRecordDate(radisRecodeDate);
		KdclStatisticsSource.setUpdateDate(date);
		KdclStatisticsSource.setCreateDate(date);
		kdclStatisticsSourceService.save(KdclStatisticsSource);
	}
	
	public static void main(String[] args) {
		boolean doFlag = true;
		if(args.length < 1){
			System.out.println("args size must > 1");
			doFlag = false;
		}else if(!args[0].equals("prd") && !args[0].equals("stg")){
			System.out.println("args[0] must prd or stg");
			doFlag = false;
		}
		
		if(doFlag){
			System.out.println("spring.profiles.active:" + args[0]);
			System.setProperty("spring.profiles.active", args[0]);
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
			KdclStatisticsSourceJob kdclStatisticsSourceJob = (KdclStatisticsSourceJob) ctx.getBean(KdclStatisticsSourceJob.class);
			kdclStatisticsSourceJob.excute();
		}
	}

}
