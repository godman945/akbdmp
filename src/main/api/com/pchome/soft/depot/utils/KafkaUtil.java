package com.pchome.soft.depot.utils;

import java.util.Random;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

@Component
public class KafkaUtil {

	Log log = LogFactory.getLog(KafkaUtil.class);

	@Autowired
	private Producer<String, String> kafkaProducer;

	public void sendMessage(String topicname, String partitionKey, String mesg) {
		try {
			Future<RecordMetadata> f = kafkaProducer.send(new ProducerRecord<String, String>(topicname, partitionKey, mesg));
				while (!f.isDone()) {
			}
			RecordMetadata recordMetadata = f.get();
//			log.info("Topic" + recordMetadata.topic() + recordMetadata.offset() + recordMetadata.partition());
		} catch (Exception e) {
			log.error(">>>>" + e.getMessage());
		}

	}

	public static void main(String[] args) {

		Log log = LogFactory.getLog(KafkaUtil.class);

		System.setProperty("spring.profiles.active", "local");

		// System.setProperty("hadoop.home.dir",
		// "d:\\nico_data\\hadoop\\hadoop-2.5.2\\hadoop-2.5.2");

		@SuppressWarnings("resource")
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		KafkaUtil kafkaUtil = (KafkaUtil) ctx.getBean(KafkaUtil.class);
//		JSONObject result2 = new JSONObject();
//		result2.put("trackId", "PCSP201603230007");
//		result2.put("pcsUserId", "PCSU201605130005");
//		result2.put("trackType", "2");
//		result2.put("create", "1");
		long time1 = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
//			Random ran = new Random();
//			int r =  ran.nextInt(10000)+1;
//			System.out.println(i);
			kafkaUtil.sendMessage("dmp_log_prd", "","{'record_count':1416,'ad_class':'2671267200000000','record_date':'2018-06-25','data':{'classify':[{'memid_kdcl_log_personal_info_api_Y':10}, {'memid_kdcl_log_personal_info_api_N':0}, {'all_kdcl_log_personal_info_Y':0}, {'all_kdcl_log_personal_info_N':5}, {'all_kdcl_log_class_ad_click_Y':20}, {'all_kdcl_log_class_ad_click_N':15}, {'all_kdcl_log_class_24h_url_Y':12}, {'all_kdcl_log_class_24h_url_N':2}, {'all_kdcl_log_class_ruten_url_Y':32}, {'all_kdcl_log_class_ruten_url_N':0}, {'all_kdcl_log_area_info_Y':3}, {'all_kdcl_log_area_info_N':3}, {'all_kdcl_log_device_info_Y':47}, {'all_kdcl_log_device_info_N':21}, {'all_kdcl_log_time_info_Y':66}, {'all_kdcl_log_time_info_N':6}],'sex_info':[{'source':'excel','value':'M','day_count':15}],'device_phone_info':[{'source':'user-agent','value':'HTC','day_count':1},{'source':'sony99-user-agent','value':'sony','day_count':1}],'device_browser_info':[{'source':'user-agent','value':'Robot/Spider','day_count':1}],'category_info':[{'source':'24h','value':'100121345564','day_count':1}],'device_os_info':[{'source':'user-agent','value':'UNKNOWN','day_count':1}],'age_info':[{'source':'null','value':'null','day_count':0}],'time_info':[{'source':'datetime','value':'16'}],'area_country_info':[{'source':'ip','value':'United States','day_count':1}],'device_info':[{'source':'user-agent','value':'UNKNOWN','day_count':1}],'area_city_info':[{'source':'ip','value':'Mountain View','day_count':1}]},'user_agent':'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)','org_source':'kdcl','date_time':'2018-05-22 16:06:53','url':'http://news.pchome.com.tw/living/ctv/20180510/video-52590680056417208009.html','key':{'uuid':'','memid':'alex_test_20180625_"+i+"'},'ip':'66.249.71.157'}");
//			kafkaUtil.sendMessage("dmp_log_prd", "", result2.toString());
		}
		long time2 = System.currentTimeMillis();
//		
		log.info(">>>>>> finish :"+ (time2- time1)+"ms");
		
//		2018/06/22 09:56:25 [INFO ] [com.pchome.soft.depot.utils.KafkaUtil@main]main(68) >>>>>> finish :52820ms
		// kafkaUtil.kafkaClose();

	}

}

