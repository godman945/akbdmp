package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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

	public RedisTemplate<String, Object> redisTemplate = null;

	public int count;

	public JSONParser jsonParser = null;

	public String redisFountKey;

	public Map<String, JSONObject> kafkaDmpMap = null;

	public Map<String, Integer> redisClassifyMap = null;

	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"
				+ context.getConfiguration().get("spring.profiles.active"));
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
			jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
			kafkaDmpMap = new HashMap<String, JSONObject>();

			String recordDate = context.getConfiguration().get("job.date");
			String env = context.getConfiguration().get("spring.profiles.active");
			if (env.equals("prd")) {
				redisFountKey = "prd:dmp:classify:" + recordDate + ":";
			} else {
				redisFountKey = "stg:dmp:classify:" + recordDate + ":";
			}

			// Classify Map
			redisClassifyMap = new HashMap<String, Integer>();
			for (EnumClassifyKeyInfo enumClassifyKeyInfo : EnumClassifyKeyInfo.values()) {
				redisClassifyMap.put(redisFountKey + enumClassifyKeyInfo.toString(), 0);
			}

		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			// log.info(">>>>>> reduce start : " + mapperKey.toString());
			String data = mapperKey.toString();
			JSONObject jsonObjOrg = (net.minidev.json.JSONObject) jsonParser.parse(data);

			// String dmpSource = (String) jsonObjOrg.get("org_source");
			String dmpMemid = (String) ((JSONObject) jsonObjOrg.get("key")).get("memid");
			String dmpUuid = (String) ((JSONObject) jsonObjOrg.get("key")).get("uuid");
			String recordDate = jsonObjOrg.getAsString("record_date");
			// 建立map key
			// StringBuffer reducerMapKey = new StringBuffer();
			// reducerMapKey.append(dmpSource);
			// reducerMapKey.append("_");
			// reducerMapKey.append(dmpMemid);
			// reducerMapKey.append("_");
			// reducerMapKey.append(dmpUuid);

			// JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
			if (((StringUtils.isNotBlank(dmpMemid) && !dmpMemid.equals("null"))
					&& ((StringUtils.isNotBlank(dmpUuid) && !dmpUuid.equals("null"))))) {
				// 先處理memid
				StringBuffer reducerMapKey = new StringBuffer();
				reducerMapKey.append(dmpMemid);
				
//				log.info("kafkaDmpMap:"+kafkaDmpMap.get(reducerMapKey.toString()));
				
				JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
				if (dmpJson == null) {
//					log.info(">>>>>>>>>1");
					processKafakDmpMapKeyNotExist(recordDate, jsonObjOrg, reducerMapKey.toString());
				} else {
//					log.info(">>>>>>>>>2");
					processKafakDmpMapKeyIsExist(recordDate, jsonObjOrg, reducerMapKey.toString(), dmpJson);
				}

				JSONObject dmpUuidJson = kafkaDmpMap.get(dmpUuid);
				if (dmpUuidJson == null) {
//					log.info(">>>>>>>>>3");
					dmpUuidJson = (JSONObject) kafkaDmpMap.get(dmpMemid).clone();
					JSONObject keyObject = (JSONObject) dmpUuidJson.get("key");
					keyObject.put("uuid", dmpUuid);
					keyObject.put("memid", "null");
				} else {
//					log.info(">>>>>>>>>4");
					dmpUuidJson = (JSONObject) kafkaDmpMap.get(dmpMemid).clone();
					JSONObject keyObject = (JSONObject) dmpUuidJson.get("key");
					keyObject.put("uuid", dmpUuid);
					keyObject.put("memid", "null");
					processKafakDmpMapKeyIsExist(recordDate, jsonObjOrg, dmpUuid.toString(), dmpUuidJson);
				}
			} else {
				StringBuffer reducerMapKey = new StringBuffer();
				if (((StringUtils.isNotBlank(dmpMemid) && !dmpMemid.equals("null")))) {
//					log.info(">>>>>>>>>7");
					reducerMapKey.append(dmpMemid);
				}
				if (((StringUtils.isNotBlank(dmpUuid) && !dmpUuid.equals("null")))) {
//					log.info(">>>>>>>>>8");
					reducerMapKey.append(dmpUuid);
				}

				JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
				if (dmpJson == null) {
//					log.info(">>>>>>>>>9");
					processKafakDmpMapKeyNotExist(recordDate, jsonObjOrg, reducerMapKey.toString());
				} else {
//					log.info(">>>>>>>>>10");
					processKafakDmpMapKeyIsExist(recordDate, jsonObjOrg, reducerMapKey.toString(), dmpJson);
				}
			}
 
			// if(dmpJson == null){
			// processKafakDmpMapKeyNotExist();
			//
			// }else{
			// processKafakDmpMapKeyIsExist();
			// }
		} catch (Throwable e) {
			// log.error(">>>>>> reduce error redis key:"
			// +reducerMapKey.toString());
			log.error("reduce error>>>>>> " + e);
			// log.error(">>>>>>reduce error>> redisClassifyMap:" +
			// redisClassifyMap);
		}
	}

	// 處理mdp map不存在時
	private void processKafakDmpMapKeyNotExist(String recordDate, JSONObject jsonObjOrg, String reducerMapKey)
			throws Exception {
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

			// 提供第3分類用
			if (StringUtils.equals(enumDataKeyInfo.name(), "category_info")) {
				infoJson.put("url", jsonObjOrg.get("url"));
			}

			array.add(infoJson);
			hadoopData.put(enumDataKeyInfo.toString(), array);
		}

		// 處理classify資訊
		JSONArray classifyArrayOrg = (JSONArray) hadoopData.get("classify");
		for (Object object : classifyArrayOrg) {
			JSONObject classifyJson = (JSONObject) object;
			for (Entry<String, Object> entry : classifyJson.entrySet()) {
				String key = (String) entry.getKey();
				key = redisFountKey + key;
				String type = (String) entry.getValue();
				if (StringUtils.equals(type, "null")) {
					break;
				} else {
					// type值是Y或N
					key = key + "_" + type;
				}
				int classifyValue = redisClassifyMap.get(key);
				classifyValue = classifyValue + 1;
				redisClassifyMap.put(key, classifyValue);
			}
		}
		kafkaDmpMap.put(reducerMapKey.toString(), jsonObjOrg);
	}

	// 處理mdp map存在時
	private void processKafakDmpMapKeyIsExist(String recordDate, JSONObject jsonObjOrg, String reducerMapKey,
			JSONObject dmpJson) throws Exception {
		
//		log.info("dmpJson:"+dmpJson.get("data"));
		
		// log.info(">>>>>>>>>10-1");
		JSONObject hadoopDataOrg = ((JSONObject) jsonObjOrg.get("data"));
		JSONObject hadoopDataDmpMap = ((JSONObject) dmpJson.get("data"));
		
		for (EnumDataKeyInfo enumDataKeyInfo : EnumDataKeyInfo.values()) {
			String source = ((JSONObject) hadoopDataOrg.get(enumDataKeyInfo.toString())).getAsString("source");
			String value = ((JSONObject) hadoopDataOrg.get(enumDataKeyInfo.toString())).getAsString("value");
			// 此次log資訊來源及值都不為null才取出資料進行判斷是否加1邏輯
			if ((StringUtils.isNotBlank(source) && !source.equals("null"))
					&& (StringUtils.isNotBlank(value) && !value.equals("null"))) {
				boolean newDetail = true;
				JSONArray array = (JSONArray) hadoopDataDmpMap.get(enumDataKeyInfo.toString());
				for (Object object : array) {
					JSONObject infoJson = (JSONObject) object;
					String kafkaDmpMapSource = infoJson.getAsString("source");
					String kafkaDmpMapValue = infoJson.getAsString("value");
					// 判斷log的source與value內容皆與kafkaDmpMap裡的內容一致，則該筆info的day_count+1
					if (source.equals(kafkaDmpMapSource) && value.equals(kafkaDmpMapValue)) {
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

					// 提供第3分類用
					if (StringUtils.equals(enumDataKeyInfo.name(), "category_info")) {
						infoJson.put("url", jsonObjOrg.get("url"));
					}
					array.add(infoJson);
				}
			}
		}
		// log.info(">>>>>>>>>10-2");
		// 計算clssify
		JSONArray orgClassifyArray = (JSONArray) hadoopDataOrg.get("classify");
		for (Object object : orgClassifyArray) {
			JSONObject orgClassifyJson = (JSONObject) object;
			for (Entry<String, Object> entry : orgClassifyJson.entrySet()) {
				String key = (String) entry.getKey();
				key = redisFountKey + key;
				String type = (String) entry.getValue();
				if (StringUtils.equals(type, "null")) {
					break;
				} else {
					// type值是Y或N
					key = key + "_" + type;
				}
				int classifyValue = redisClassifyMap.get(key);
				classifyValue = classifyValue + 1;
				redisClassifyMap.put(key, classifyValue);
			}
		}
		// log.info(">>>>>>>>>10-3");
		kafkaDmpMap.put(reducerMapKey.toString(), dmpJson);
	}

	
	private static String partitionHashcode = "1";
	private static int partition = 0;
	private static int total = 0;
	public void cleanup(Context context) {
		try {
			String kafkaTopic;
			String env = context.getConfiguration().get("spring.profiles.active");
			if (env.equals("prd")) {
				kafkaTopic = "dmp_log_prd";
			} else {
				kafkaTopic = "dmp_log_stg";
			}
			Iterator iterator = kafkaDmpMap.entrySet().iterator();
			while (iterator.hasNext()) {
				count = count + 1;
				Map.Entry mapEntry = (Map.Entry) iterator.next();
				if(count == 1){
					log.info("mapEntry:"+mapEntry);
					log.info("mapEntry size:"+kafkaDmpMap.size());
				}
				producer.send(new ProducerRecord<String, String>(kafkaTopic, partitionHashcode, mapEntry.getValue().toString()));
//				Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>(kafkaTopic, partitionHashcode, mapEntry.getValue().toString()));
//				while (!f.isDone()) {
//					
//				}
				log.info("process count:"+count);
				if(partition == 2){
					partition = 0;
					partitionHashcode = "1";
				}else{
					partition = partition + 1;
					if(partition == 1){
						partitionHashcode = "key0";
					}
					if(partition == 2){
						partitionHashcode = "key2";
					}
				}
			}
			producer.close();
			log.info(">>>>>>reduce count:" + count);
			log.info(">>>>>>write clssify to Redis>>>>>");
			log.info(">>>>>>cleanup redisClassifyMap:" + redisClassifyMap);
			for (Entry<String, Integer> redisMap : redisClassifyMap.entrySet()) {
				String redisKey = redisMap.getKey();
				total = 0;
				total = redisMap.getValue();
				redisTemplate.opsForValue().increment(redisKey, total);
				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
			}
		} catch (Throwable e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
		// mark

		// try {
		//// log.info(">>>>>>write cleanup>>>>>");
		//
		// String kafkaTopic;
		// String env =
		// context.getConfiguration().get("spring.profiles.active");
		// if(env.equals("prd")){
		// kafkaTopic = "dmp_log_prd";
		// }else{
		// kafkaTopic = "dmp_log_stg";
		// }
		// log.info(">>>>>>kafkaTopic: " + kafkaTopic);
		//
		// Iterator iterator = kafkaDmpMap.entrySet().iterator();
		// while (iterator.hasNext()) {
		// count = count + 1;
		// Map.Entry mapEntry = (Map.Entry) iterator.next();
		// Future<RecordMetadata> f = producer.send(new ProducerRecord<String,
		// String>(kafkaTopic, "", mapEntry.getValue().toString()));
		// while (!f.isDone()) {
		// }
		//// keyOut.set(mapEntry.getValue().toString());
		//// context.write(keyOut, valueOut);
		//// log.info(">>>>>>reduce Map send kafka:" +
		// mapEntry.getValue().toString());
		// }
		// producer.close();
		// log.info(">>>>>>reduce count:" + count);
		// log.info(">>>>>>write clssify to Redis>>>>>");
		// log.info(">>>>>>cleanup redisClassifyMap:" + redisClassifyMap);
		// for (Entry<String, Integer> redisMap : redisClassifyMap.entrySet()) {
		// String redisKey = redisMap.getKey();
		// int count = redisMap.getValue();
		// redisTemplate.opsForValue().increment(redisKey, count);
		// redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
		// }
		// } catch (Throwable e) {
		// log.error("reduce cleanup error>>>>>> " + e);
		// }

	}
}
