package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;
import org.json.JSONArray;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.IKdclStatisticsSourceService;
import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.KdclStatisticsSourceService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class DmpLogReducer extends Reducer<Text, Text, Text, Text> {

	Log log = LogFactory.getLog("DmpLogReducer");

	SimpleDateFormat sdf = null;
	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	public static String record_date;

	private String kafkaMetadataBrokerlist;

	private String kafkaAcks;

	private String kafkaRetries;

	private String kafkaBatchSize;

	private String kafkaLingerMs;

	private String kafkaBufferMemory;

	private String kafkaSerializerClass;

	private String kafkaKeySerializer;

	private String kafkaValueSerializer;

	private String environment;
	
	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;

	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {
			this.sdf = new SimpleDateFormat("yyyy-MM-dd");
			if(StringUtils.isNotBlank(context.getConfiguration().get("spring.profiles.active"))){
				System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
				this.environment = "prd";
			}else{
				System.setProperty("spring.profiles.active", "stg");
				this.environment = "stg";
			}
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.kafkaMetadataBrokerlist = ctx.getEnvironment().getProperty("kafka.metadata.broker.list");
			this.kafkaAcks = ctx.getEnvironment().getProperty("kafka.acks");
			this.kafkaRetries = ctx.getEnvironment().getProperty("kafka.retries");
			this.kafkaBatchSize = ctx.getEnvironment().getProperty("kafka.batch.size");
			this.kafkaLingerMs = ctx.getEnvironment().getProperty("kafka.linger.ms");
			this.kafkaBufferMemory = ctx.getEnvironment().getProperty("kafka.buffer.memory");
			this.kafkaSerializerClass = ctx.getEnvironment().getProperty("kafka.serializer.class");
			this.kafkaKeySerializer = ctx.getEnvironment().getProperty("kafka.key.serializer");
			this.kafkaValueSerializer = ctx.getEnvironment().getProperty("kafka.value.serializer");
			
			Properties props = new Properties();
			props.put("bootstrap.servers", kafkaMetadataBrokerlist);
			props.put("acks", kafkaAcks);
			props.put("retries", kafkaRetries);
			props.put("batch.size", kafkaBatchSize);
			props.put("linger.ms", kafkaLingerMs);
			props.put("buffer.memory", kafkaBufferMemory);
			props.put("serializer.class", kafkaSerializerClass);
			props.put("key.serializer", kafkaKeySerializer);
			props.put("value.serializer", kafkaValueSerializer);
			producer = new KafkaProducer<String, String>(props);

		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		// 0:memid + 1:uuid + 2:adClass + 3.adClassSource
		// 4.sex + 5.sexSource + 6.age + 7.ageSource
		// 8.country + 9.city + 10.areaInfoSource
		// 11.device_info_source + 12.device_info
		// 13.device_phone_info + 14.device_os_info + 15.device_browser_info
		// 16.time_info + 17.time_info_source + 18.url
		try {
			log.info(">>>>>> reduce start : " + key);

			String data[] = key.toString().split(SYMBOL);

			//send kafka key
			JSONObject keyJson = new JSONObject();
			keyJson.put("memid", data[0]);
			keyJson.put("uuid", data[1]);

			//send kafka data
			//category_info
			Map categoryInfoMap = new HashMap();
			categoryInfoMap.put("value", data[2]);
			categoryInfoMap.put("source", data[3]);

			//sex_info
			Map sexInfoMap = new HashMap();
			sexInfoMap.put("value", data[4]);
			sexInfoMap.put("source", data[5]);

			//age_info
			Map ageInfoMap = new HashMap();
			ageInfoMap.put("value", data[6]);
			ageInfoMap.put("source", data[7]);

			//area_country_info
			Map areaCountryInfoMap = new HashMap();
			areaCountryInfoMap.put("value", data[8]);
			areaCountryInfoMap.put("source", data[10]);
			
			//area_city_info
			Map areaCityInfoMap = new HashMap();
			areaCityInfoMap.put("value", data[9]);
			areaCityInfoMap.put("source", data[10]);
			
			//device_info
			Map deviceInfoMap = new HashMap();
			deviceInfoMap.put("value", data[12]);
			deviceInfoMap.put("source", data[11]);

			//device_phone_info
			Map devicePhoneInfoMap = new HashMap();
			devicePhoneInfoMap.put("value", data[13]);
			devicePhoneInfoMap.put("source", data[11]);

			//device_os_info
			Map deviceOsInfoMap = new HashMap();
			deviceOsInfoMap.put("value", data[14]);
			deviceOsInfoMap.put("source", data[11]);

			//device_browser_info
			Map deviceBrowserInfoMap = new HashMap();
			deviceBrowserInfoMap.put("value", data[15]);
			deviceBrowserInfoMap.put("source", data[11]);

			//time_info
			Map timeInfoMap = new HashMap();
			timeInfoMap.put("value", data[16]);
			timeInfoMap.put("source", data[17]);

			
			//classify Array
			JSONArray classifyArray = new JSONArray();
			
			Map all_kdcl_log_class_ad_click_map = new HashMap();
			all_kdcl_log_class_ad_click_map.put("all_kdcl_log_class_ad_click", "Y");
			classifyArray.put(all_kdcl_log_class_ad_click_map);

			Map memid_member_api_personal_info_api_map = new HashMap();
			memid_member_api_personal_info_api_map.put("memid_member_api_personal_info_api", "N");
			classifyArray.put(memid_member_api_personal_info_api_map);
			
			Map memid_kdcl_log_personal_info_map = new HashMap();
			memid_kdcl_log_personal_info_map.put("memid_kdcl_log_personal_info_map", "N");
			classifyArray.put(memid_kdcl_log_personal_info_map);
			
			
			//dataJson
			JSONObject dataJson = new JSONObject();
			dataJson.put("category_info", categoryInfoMap);
			dataJson.put("sex_info", sexInfoMap);
			dataJson.put("age_info", ageInfoMap);
			dataJson.put("area_country_info", areaCountryInfoMap );
			dataJson.put("area_city_info", areaCityInfoMap );
			dataJson.put("device_info", deviceInfoMap );
			dataJson.put("device_phone_info", devicePhoneInfoMap );
			dataJson.put("device_os_info", deviceOsInfoMap);
			dataJson.put("device_browser_info", deviceBrowserInfoMap);
			dataJson.put("time_info", timeInfoMap);
			dataJson.put("classify", classifyArray);

			
			//send Kafka Json
			JSONObject sendKafkaJson = new JSONObject();
			sendKafkaJson.put("key", keyJson);
			sendKafkaJson.put("data", dataJson);

//			System.out.println(sendKafkaJson.toString());
			
			if(this.environment.equals("prd")){
				Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>("dmp_log_prd", "", sendKafkaJson.toString()));
				 while (!f.isDone()) {
				 }
			}else{
				Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>("dmp_log_stg", "", sendKafkaJson.toString()));
				 while (!f.isDone()) {
				 }
			}
			
			log.info(">>>>>>reduce write key:" + sendKafkaJson.toString());
			
			keyOut.set(key);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.info("reduce error"+e.getMessage());
			log.error(key, e);
		}

	}

	public void cleanup(Context context) {
		try {
			producer.close();

		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	
//	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//		Date date = new Date();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		
//		
////		int a = 0;
////		Calendar calendar = Calendar.getInstance();  
////		if(calendar.get(Calendar.HOUR_OF_DAY) == 16){
////			calendar.add(Calendar.DAY_OF_MONTH, -1); 
////			System.out.println(sdf.format(calendar.getTime()));;
////		}
////		KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("7", sdf.format(calendar.getTime()));
////		a = a + kdclStatisticsSource.getCounter();
////		
////		System.out.println(a);
////		
////		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("7", sdf.format(calendar.getTime()));
////		
////		
////		a = a + 10;
////		
//		
//		
////		String recodeDate = sdf.format(date);
////		System.out.println(recodeDate);
////		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ad_click", recodeDate);
////		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("24h", recodeDate);
////		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ruten", recodeDate);
////		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("personal_info", recodeDate);
//		
//		
////		UserDetailService userDetailService = (UserDetailService) ctx.getBean(UserDetailService.class);
////		UserDetailMongoBean userDetailMongoBean = userDetailService.findUserId("zxc910615");
////		
////		System.out.println(userDetailMongoBean.get_id());
////		System.out.println((String)userDetailMongoBean.getUser_info().get("sex"));
//		
//		
//		
//		
//		
//		
//		
////		Query query = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
////		UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(query, UserDetailMongoBean.class);
////		userDetailMongoBean.getUser_info().get("sex")
////		userDetailMongoBean.getUser_info().get("age")
//		
//		
//		
//		
////		System.out.println(userDetailMongoBean.getCategory_info().get(0).get("category"));
//		
//		
////		KdclStatisticsSource kdclStatisticsSource = new KdclStatisticsSource();
////		kdclStatisticsSource.setClassify("A");
////		kdclStatisticsSource.setIdType("alex");
////		kdclStatisticsSource.setServiceType("9");
////		kdclStatisticsSource.setBehavior("GGG");
////		kdclStatisticsSource.setCounter(0);
////		kdclStatisticsSource.setRecordDate("2018-01-25");
////		kdclStatisticsSource.setUpdateDate(new Date());
////		kdclStatisticsSource.setCreateDate(new Date());
////		kdclStatisticsSourceService.save(kdclStatisticsSource);
//	}


	
}
