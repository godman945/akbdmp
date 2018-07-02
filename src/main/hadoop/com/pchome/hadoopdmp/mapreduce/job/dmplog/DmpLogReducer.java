package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class DmpLogReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("DmpLogReducer");
	
//	public static Map<String, Object> dmpLogMap = new HashMap<String, Object>();
	
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

	public static Producer<String, String> producer = null;

//	public StringBuffer reducerMapKey = new StringBuffer();
	
	public RedisTemplate<String, Object> redisTemplate = null;
	
	public Set<String> redisKeySet = null;
	
	public long start;
	
	public long times;
	
	public int count;
	
	public JSONParser jsonParser = null;
	
	public String redisFountKey;
	
	public Map<String,JSONObject> kafkaDmpMap =null;
	
	public Map<String,Integer> reduceDmpMap =null;
	
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
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
			redisKeySet = new HashSet<String>();
			start = System.currentTimeMillis();
			times = 0;
			jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
			kafkaDmpMap = new HashMap<String,JSONObject>();
			
			String recordDate = context.getConfiguration().get("job.date");
			String env = context.getConfiguration().get("spring.profiles.active");
			if(env.equals("prd")){
				redisFountKey = "prd:dmp:classify:"+recordDate+":";
			}else{
				redisFountKey = "stg:dmp:classify:"+recordDate+":";
			}
			
			//Classify Map
			reduceDmpMap = new HashMap<String,Integer>();
			for (EnumClassifyKeyInfo enumClassifyKeyInfo : EnumClassifyKeyInfo.values()) {
				reduceDmpMap.put(redisFountKey+enumClassifyKeyInfo.toString(), 0);
			}
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " +e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
//			log.info(">>>>>> reduce start : " + data);
			String data = mapperKey.toString();
			JSONObject jsonObjOrg = (net.minidev.json.JSONObject)jsonParser.parse(data);
			
			String dmpSource = (String) jsonObjOrg.get("org_source");
			String dmpMemid =  (String) ((JSONObject) jsonObjOrg.get("key")).get("memid");
			String dmpUuid = (String) ((JSONObject) jsonObjOrg.get("key")).get("uuid");
			String recordDate = jsonObjOrg.getAsString("record_date");
			//建立map key
			StringBuffer reducerMapKey = new StringBuffer();
			reducerMapKey.append(dmpSource);
			reducerMapKey.append("_");
			reducerMapKey.append(dmpMemid);
			reducerMapKey.append("_");
			reducerMapKey.append(dmpUuid);
			
			JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
			
			
			
			if(dmpJson == null){
				// 處理info資訊
				JSONObject hadoopData = ((JSONObject) jsonObjOrg.get("data"));
				hadoopData.put("record_date", recordDate);
				for (EnumDataKeyInfo enumDataKeyInfo : EnumDataKeyInfo.values()) {
					JSONArray array = new JSONArray();
					JSONObject infoJson = (JSONObject) hadoopData.get(enumDataKeyInfo.toString());
					String source = infoJson.getAsString("source");
					String value = infoJson.getAsString("value");
					if ((StringUtils.isNotBlank(source) && !source.equals("null"))
							&& (StringUtils.isNotBlank(value) && !value.equals("null"))) {
						infoJson.put("day_count", 1);
					} else {
						infoJson.put("day_count", 0);
					}
					array.add(infoJson);
					hadoopData.put(enumDataKeyInfo.toString(), array);
				}

//				// 處理classify資訊
//				JSONArray classifyArrayOrg = (JSONArray) hadoopData.get("classify");
//				JSONArray classifyArray = new JSONArray();
//				JSONObject classifyObj = new JSONObject();
//				for (Object object : classifyArrayOrg) {
//					JSONObject classifyJson = (JSONObject) object;
//					for (Entry<String, Object> entry : classifyJson.entrySet()) {
//						String key = (String) entry.getKey();
//						String nKey = key + "_" + "N";
//						String yKey = key + "_" + "Y";
//						String type = (String) entry.getValue();
//						if (StringUtils.equals(type, "null")) {
//							classifyObj.put(yKey, 0);
//							classifyObj.put(nKey, 0);
//						} else if (type.equals("Y")) {
//							classifyObj.put(yKey, 1);
//							classifyObj.put(nKey, 0);
//						} else if (type.equals("N")) {
//							classifyObj.put(yKey, 0);
//							classifyObj.put(nKey, 1);
//						}
//					}
//					
//				}
//				classifyArrayOrg.clear();
//				classifyArray.add(classifyObj);
//				hadoopData.put("classify", classifyArray);
//				jsonObjOrg.put("data", hadoopData);
				
				//new
				JSONArray classifyArrayOrg = (JSONArray) hadoopData.get("classify");
				for (Object object : classifyArrayOrg) {
					JSONObject classifyJson = (JSONObject) object;
					for (Entry<String, Object> entry : classifyJson.entrySet()) {
						String key = (String) entry.getKey();
						
						key = redisFountKey + key;
						
						String type = (String) entry.getValue();
						if (StringUtils.equals(type, "null")) {
							break;
						} else{
							key = key +"_"+type;
						} 
						int classifyValue = reduceDmpMap.get(key);
						classifyValue = classifyValue + 1;
						reduceDmpMap.put(key, classifyValue);
					}
				}
				//new
				
				
				kafkaDmpMap.put(reducerMapKey.toString(), jsonObjOrg);
			}else{
				JSONObject hadoopDataOrg = ((JSONObject) jsonObjOrg.get("data"));
				JSONObject hadoopDataRedis = ((JSONObject) dmpJson.get("data"));
				for (EnumDataKeyInfo enumDataKeyInfo : EnumDataKeyInfo.values()) {
					String source = ((JSONObject) hadoopDataOrg.get(enumDataKeyInfo.toString())).getAsString("source");
					String value = ((JSONObject) hadoopDataOrg.get(enumDataKeyInfo.toString())).getAsString("value");
					// 此次log資訊來源及值都不為null才取出資料進行判斷是否加1邏輯
					if ((StringUtils.isNotBlank(source) && !source.equals("null"))
							&& (StringUtils.isNotBlank(value) && !value.equals("null"))) {
						boolean newDetail = true;
						JSONArray array = (JSONArray) hadoopDataRedis.get(enumDataKeyInfo.toString());
						for (Object object : array) {
							JSONObject infoJson = (JSONObject) object;
							String redisSource = infoJson.getAsString("source");
							String redisValue = infoJson.getAsString("value");
							// 判斷log與redis內容是否一致
							if (source.equals(redisSource) && value.equals(redisValue)) {
								int dayCount = (int) infoJson.get("day_count");
								dayCount = dayCount + 1;
								infoJson.put("day_count", dayCount);
								newDetail = false;
							}

						}
						// 比對不到加入info所屬陣列
						if (newDetail) {
							JSONObject infoJson = new JSONObject();
							infoJson.put("source", source);
							infoJson.put("value", value);
							infoJson.put("day_count", 1);
							array.add(infoJson);
						}
					}
				}

				
				
//				// 計算clssify
//				JSONArray redisClassifyArray = (JSONArray) hadoopDataRedis.get("classify");
//				JSONArray orgClassifyArray = (JSONArray) hadoopDataOrg.get("classify");
//				for (Object object : orgClassifyArray) {
//					JSONObject orgClassifyJson = (JSONObject) object;
//					for (Entry<String, Object> entry : orgClassifyJson.entrySet()) {
//						String key = (String) entry.getKey();
//						String nKey = key + "_" + "N";
//						String yKey = key + "_" + "Y";
//						String type = (String) entry.getValue();
//						if (type.equals("Y")) {
//							resetCountClassify(yKey, redisClassifyArray);
//						} else if (type.equals("N")) {
//							resetCountClassify(nKey, redisClassifyArray);
//						}
//					}
//				}
				
				//new
				JSONArray orgClassifyArray = (JSONArray) hadoopDataOrg.get("classify");
				for (Object object : orgClassifyArray) {
					JSONObject orgClassifyJson = (JSONObject) object;
					for (Entry<String, Object> entry : orgClassifyJson.entrySet()) {
						String key = (String) entry.getKey();
						
						key = redisFountKey + key; 
						
						String type = (String) entry.getValue();
						if (StringUtils.equals(type, "null")) {
							break;
						} else{
							key = key +"_"+type;
						} 
						int classifyValue = reduceDmpMap.get(key);
						classifyValue = classifyValue + 1;
						reduceDmpMap.put(key, classifyValue);
					}
				}
				//new
				
				
				
				kafkaDmpMap.put(reducerMapKey.toString(), dmpJson);
			}
			//清空
//			reducerMapKey.setLength(0);
		} catch (Throwable e) {
//			log.error(">>>>>> reduce error redis key:" +reducerMapKey.toString());
//			reducerMapKey.setLength(0);
			
			log.error("reduce error>>>>>> " +e);
			log.info(">>>>>>reduce error>> redisClassifyMap:" + reduceDmpMap);
			
		}
	}
	
	//更新redis中classify數字
	public void resetCountClassify(String key,JSONArray redisClassify){
		for (Object object : redisClassify) {
			JSONObject classifyJson = (JSONObject) object;
			Integer count = (Integer) classifyJson.get(key);
			if(count != null){
				count = count + 1;
				classifyJson.put(key, count);
				break;
			}
		}
	}

	
	@SuppressWarnings("rawtypes")
	public void processRedisMap(Map.Entry mapEntry,Map<String,Integer> redisClassifyMap){
		JSONArray classifyArray = (JSONArray) ((Map)((Map)mapEntry.getValue()).get("data")).get("classify");
		for (Object object : classifyArray) {
			JSONObject obj = (JSONObject) object;
			for (Entry<String, Object> set : obj.entrySet()) {
				String redisKey = this.redisFountKey+set.getKey();
				int redisCount = (int) set.getValue();
				if(redisClassifyMap.containsKey(redisKey)){
					int orgRedisCount = redisClassifyMap.get(redisKey);
					int newRedisCount = redisCount + orgRedisCount;
					redisClassifyMap.put(redisKey, newRedisCount);
				}else{
					redisClassifyMap.put(redisKey, redisCount);
				}
			}
		}
	}
	
	public void cleanup(Context context) {
		try {
			log.info(">>>>>>write cleanup>>>>>");
			
			Map<String,Integer> redisClassifyMap = new HashMap<String, Integer>();
			Iterator iterator = kafkaDmpMap.entrySet().iterator();
			while (iterator.hasNext()) {
				count = count+1;
				Map.Entry mapEntry = (Map.Entry) iterator.next();
				producer.send(new ProducerRecord<String, String>("dmp_log_prd", "", mapEntry.getValue().toString()));
//				keyOut.set(mapEntry.getValue().toString());
//				context.write(keyOut, valueOut);
				
				
//				//處理redis
//				processRedisMap(mapEntry,redisClassifyMap);
				
				
//				log.info(">>>>>>reduce Map send kafka:" + mapEntry.getValue().toString());
			}
			log.info(">>>>>>write Redis>>>>>");
			
			
			//temp mark
//			for (Entry<String, Integer> redisMap : reduceDmpMap.entrySet()) {
//				String redisKey =redisMap.getKey();
//				int count = redisMap.getValue();
//				redisTemplate.opsForValue().increment(redisKey, count);
//				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
//			}
			//temp mark
			
			
			
//			for (Entry<String, Integer> redisMap : redisClassifyMap.entrySet()) {
//				String redisKey = redisMap.getKey();
//				int count = redisMap.getValue();
//				redisTemplate.opsForValue().increment(redisKey, count);
//				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
//			}
			log.info(">>>>>>reduce count:" + count);
			log.info(">>>>>>cleanup redisClassifyMap:" + reduceDmpMap);
			for (Entry<String, Integer> redisMap : reduceDmpMap.entrySet()) {
				String redisKey = redisMap.getKey();
				int count = redisMap.getValue();
				redisTemplate.opsForValue().increment(redisKey, count);
				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
			}
			producer.close();
		} catch (Throwable e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}

	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		DmpLogReducer dmpLogReducer = ctx.getBean(DmpLogReducer.class);
////		String a ="{'key':{'memid':'null','uuid':'c014b82c-65c3-4e59-88ac-825152412307'},'data':{'category_info':{'source':'null','value':'null'},'sex_info':{'source':'null','value':'null'},'age_info':{'source':'null','value':'null'},'area_country_info':{'source':'ip','value':'Taiwan'},'area_city_info':{'source':'ip','value':'Chengde'},'device_info':{'source':'user-agent','value':'MOBILE'},'device_phone_info':{'source':'user-agent','value':'APPLE'},'device_os_info':{'source':'user-agent','value':'IOS'},'device_browser_info':{'source':'user-agent','value':'Mobile Safari'},'time_info':{'source':'datetime','value':'17'},'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'Y'},{'all_kdcl_log_time_info':'Y'}]},'url':'','ip':'101.139.176.46','record_date':'2018-06-11','org_source':'kdcl','date_time':'2018-05-22 17:07:44','user_agent':'Mozilla\\5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) AppleWebKit\\604.1.34 (KHTML, like Gecko) GSA\\37.1.171590344 Mobile\\15E302 Safari\\604.1','ad_class':'0017024822530000','record_count':658}";
//		String a ="{'key':{'memid':'null','uuid':'c014b82c-65c3-4e59-88ac-825152412307'},'data':{'category_info':{'source':24h,'value':7997979979},'sex_info':{'source':'null','value':'null'},'age_info':{'source':'null','value':'null'},'area_country_info':{'source':'ip','value':'Taiwan'},'area_city_info':{'source':'ip','value':'Chengde'},'device_info':{'source':'user-agent','value':'MOBILE'},'device_phone_info':{'source':'user-agent','value':'APPLE'},'device_os_info':{'source':'user-agent','value':'IOS'},'device_browser_info':{'source':'user-agent','value':'Mobile Safari'},'time_info':{'source':'datetime','value':'17'},'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'Y'},{'all_kdcl_log_time_info':'Y'}]},'url':'http:\\\\www.gaomei.com.tw\\openhours\','ip':'101.139.176.46','record_date':'2018-06-11','org_source':'kdcl','date_time':'2018-05-22 17:05:53','user_agent':'Mozilla\\5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) AppleWebKit\\604.1.34 (KHTML, like Gecko) GSA\\37.1.171590344 Mobile\\15E302 Safari\\604.1','ad_class':'0017024822530000','record_count':287}";
//		JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
//		net.minidev.json.JSONObject json = (net.minidev.json.JSONObject) jsonParser.parse(a);
//		for (int i = 0; i < 1; i++) {
//			dmpLogReducer.reduce(new Text(json.toString()), null, null);
//		}	
		
		
		RedisTemplate<String, Object> redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
		System.out.println(redisTemplate.opsForValue().get("kdcl_null_d9228b87-bc6d-4e23-a4e6-0d7563380c7e"));
//		redisTemplate.delete("kdcl_null_c014b82c-65c3-4e59-88ac-825152412307");
//		List<String> a = new ArrayList<>();
//		a.add("a");
//		a.add("b");
//		a.add("c");
//		
		
//		StringBuffer f = new StringBuffer();
//		f.append("ALEX");
//		
//		
//		String c = f.toString();
//		
//		f.setLength(0);
//		
//		System.out.println(c);
		
//		RedisTemplate<String, Object> redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
////		redisTemplate.opsForValue().set("alex", "123");
//		redisTemplate.expire("alex", 60, TimeUnit.SECONDS);
//		System.out.println(redisTemplate.opsForValue().get("alex"));
		
		
//		String[] array = {"alex","TEST"};
			
		
		
		
		
//		System.out.println(json);
//		JSONObject v = new JSONObject(a);
//		System.out.println(v);

//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
}

//	
}
