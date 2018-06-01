package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.util.ArrayList;
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

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class DmpLogReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("DmpLogReducer");
	
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

	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;

	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
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
			log.error("reduce setup error>>>>>> " +e);
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		// 0:memid + 1:uuid + 2:category + 3.categorySource
		// 4.sex + 5.sexSource + 6.age + 7.ageSource
		// 8.country + 9.city + 10.areaInfoSource
		// 11.device_info_source + 12.device_info
		// 13.device_phone_info + 14.device_os_info + 15.device_browser_info
		// 16.time_info_hour + 17.time_info_source

		// classify
		// 18.personal_info_api + 19.personal_info
		// 20.class_ad_click + 21.class_24h_url + 22.class_ruten_url
		// 23.area_info + 24.device_info + 25.time_info
		//26.url + 27.ip + 28.record_date + 29.org_source(kdcl、campaign) 
		//30.date_time + 31.user_agent +32.ad_class + 33.record_count
		try {
//			log.info(">>>>>> reduce start : " + key);

			String data[] = key.toString().split(SYMBOL);
			
			
			if( StringUtils.equals(data[0], "null") && StringUtils.equals(data[1], "null")){
				return;
			}

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

			
			//put classify Array
			JSONArray classifyArray = new JSONArray();
			
			//kdcl classify 
			if ( (StringUtils.equals(data[29], "kdcl")) ){
				
				//memid_kdcl_log_personal_info_api
				Map memid_kdcl_log_personal_info_api = new HashMap();
				memid_kdcl_log_personal_info_api.put("memid_kdcl_log_personal_info_api", data[18]);
				classifyArray.put(memid_kdcl_log_personal_info_api);
				
				//all_kdcl_log_personal_info
				Map all_kdcl_log_personal_info = new HashMap();
				all_kdcl_log_personal_info.put("all_kdcl_log_personal_info", data[19]);
				classifyArray.put(all_kdcl_log_personal_info);
				
				//all_kdcl_log_class_ad_click
				Map all_kdcl_log_class_ad_click = new HashMap();
				all_kdcl_log_class_ad_click.put("all_kdcl_log_class_ad_click", data[20]);
				classifyArray.put(all_kdcl_log_class_ad_click);
				
				//all_kdcl_log_class_24h_url
				Map all_kdcl_log_class_24h_url = new HashMap();
				all_kdcl_log_class_24h_url.put("all_kdcl_log_class_24h_url", data[21]);
				classifyArray.put(all_kdcl_log_class_24h_url);
				
				//all_kdcl_log_class_ruten_url
				Map all_kdcl_log_class_ruten_url = new HashMap();
				all_kdcl_log_class_ruten_url.put("all_kdcl_log_class_ruten_url", data[22]);
				classifyArray.put(all_kdcl_log_class_ruten_url);
				
				//all_kdcl_log_area_info
				Map all_kdcl_log_area_info = new HashMap();
				all_kdcl_log_area_info.put("all_kdcl_log_area_info", data[23]);
				classifyArray.put(all_kdcl_log_area_info);
				
				//all_kdcl_log_device_info
				Map all_kdcl_log_device_info = new HashMap();
				all_kdcl_log_device_info.put("all_kdcl_log_device_info", data[24]);
				classifyArray.put(all_kdcl_log_device_info);
				
				//all_kdcl_log_time_info
				Map all_kdcl_log_time_info = new HashMap();
				all_kdcl_log_time_info.put("all_kdcl_log_time_info", data[25]);
				classifyArray.put(all_kdcl_log_time_info);
				
			}
			
			//campaign classify
			if ( (StringUtils.equals(data[29], "campaign")) ){
				//memid_camp_log_personal_info_api
				Map memid_camp_log_personal_info_api = new HashMap();
				memid_camp_log_personal_info_api.put("memid_camp_log_personal_info_api", data[18]);
				classifyArray.put(memid_camp_log_personal_info_api);
				
				//all_camp_log_personal_info
				Map all_camp_log_personal_info = new HashMap();
				all_camp_log_personal_info.put("all_camp_log_personal_info", data[19]);
				classifyArray.put(all_camp_log_personal_info);
				
				//all_camp_log_class_ad_click
				Map all_camp_log_class_ad_click = new HashMap();
				all_camp_log_class_ad_click.put("all_camp_log_class_ad_click", data[20]);
				classifyArray.put(all_camp_log_class_ad_click);
				
				//all_camp_log_area_info
				Map all_camp_log_area_info = new HashMap();
				all_camp_log_area_info.put("all_camp_log_area_info", data[23]);
				classifyArray.put(all_camp_log_area_info);
				
				//all_camp_log_device_info
				Map all_camp_log_device_info = new HashMap();
				all_camp_log_device_info.put("all_camp_log_device_info", data[24]);
				classifyArray.put(all_camp_log_device_info);

				//all_camp_log_time_info
				Map all_camp_log_time_info = new HashMap();
				all_camp_log_time_info.put("all_camp_log_time_info", data[25]);
				classifyArray.put(all_camp_log_time_info);
			}

			
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
			sendKafkaJson.put("url", data[26]);
			sendKafkaJson.put("ip", data[27]);
			sendKafkaJson.put("record_date", data[28]);
			sendKafkaJson.put("org_source", data[29]);
			sendKafkaJson.put("date_time", data[30]);
			sendKafkaJson.put("user_agent", data[31]);
			sendKafkaJson.put("ad_class", data[32]);
			sendKafkaJson.put("record_count", data[33]);
			
			Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>("dmp_log_prd", "", sendKafkaJson.toString()));
			
			log.info(">>>>>>reduce write key:" + sendKafkaJson.toString());
			
			keyOut.set(sendKafkaJson.toString());
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error("reduce error>>>>>> " +e);
		}

	}

	public void cleanup(Context context) {
		try {
			producer.close();
		} catch (Exception e) {
			log.error("reduce cleanup error>>>>>> " +e);
		}
	}

	
//	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//}

//	
}
