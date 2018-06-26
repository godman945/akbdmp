package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class DmpLogReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("DmpLogReducer");
	
	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	
	public static Map<String, Object> dmpLogMap = new HashMap<String, Object>();
	
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

		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " +e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
//			log.info(">>>>>> reduce start : " + key);
			String data = mapperKey.toString();
			
			
			JSONObject jsonObjOrg = new JSONObject(data);
			String orgSource = (String) jsonObjOrg.get("org_source");
			JSONObject keyOrg = (JSONObject) jsonObjOrg.get("key");
			String memidOrg = (String) keyOrg.get("memid");
			String uuidOrg = (String) keyOrg.get("uuid");

			StringBuffer reducerMapKey = new StringBuffer(orgSource);
			reducerMapKey.append("_");
			reducerMapKey.append(memidOrg);
			reducerMapKey.append("_");
			reducerMapKey.append(uuidOrg);

			JSONObject dataOrg = (JSONObject) jsonObjOrg.get("data");
			// 組新json
			if (dmpLogMap.get(reducerMapKey.toString()) == null) {// MAP沒資料，新建資料
//				System.out.println("-------------------沒有------MAP--------------------------------: ");
				// put data
				JSONObject jsonObjAll = new JSONObject();
				// put info array
				JSONObject jsonObjInfo = new JSONObject();
				// key
				JSONObject keyJson = new JSONObject();
				keyJson.put("memid", memidOrg);
				keyJson.put("uuid", uuidOrg);
				jsonObjAll.put("key", keyJson);

				for (EnumDataKeyInfo enumDataKeyInfo : EnumDataKeyInfo.values()) {
					String dataInfoStr = enumDataKeyInfo.toString();
					String dataInfoSource = (dataOrg.getJSONObject(dataInfoStr)).getString("source");
					String dataInfoValue = (dataOrg.getJSONObject(dataInfoStr)).getString("value");
					JSONObject sexInfoObj = new JSONObject();
					sexInfoObj.put("source", dataInfoSource);
					sexInfoObj.put("value", dataInfoValue);
					sexInfoObj.put("day_count", StringUtils.equals(dataInfoValue, "null") ? 0 : 1);
					JSONArray dataInfoArray = new JSONArray();
					dataInfoArray.put(sexInfoObj);
					jsonObjInfo.put(dataInfoStr, dataInfoArray);
				}
				// 放全將data info
				jsonObjAll.put("data", jsonObjInfo);

				// 整理classify
				JSONArray classifyAryOrg = new JSONArray();
				classifyAryOrg = dataOrg.getJSONArray("classify");
				JSONObject classifyYjson = null;
				JSONObject classifyNjson = null;
				JSONArray classifyAryAll = new JSONArray();
				for (Object object : classifyAryOrg) {
					Iterator iterator = ((JSONObject) object).keys();
					while (iterator.hasNext()) {
						String key = (String) iterator.next();
						String value = ((JSONObject) object).getString(key);
						int day_count = 0;

						classifyYjson = new JSONObject();
						classifyYjson.put(key + "_Y", day_count);
						classifyNjson = new JSONObject();
						classifyNjson.put(key + "_N", day_count);

						if (StringUtils.equals(value, "Y")) {
							classifyYjson.put(key + "_Y", 1);
						}
						classifyAryAll.put(classifyYjson);

						if (StringUtils.equals(value, "N")) {
							classifyNjson.put(key + "_N", 1);
						}
						classifyAryAll.put(classifyNjson);
					}
				}
				jsonObjAll.put("classify", classifyAryAll);
				jsonObjAll.put("record_date", jsonObjOrg.get("record_date"));
				dmpLogMap.put(reducerMapKey.toString(), jsonObjAll.toString());
//				System.out.println("第1次dmpLogMap---: " + dmpLogMap);
			}

			// dmpLogMap.get(reducerMapKey.toString()) != null
			else {// MAP有資料
//				System.out.println("---------------------有有有有有有MAP--------------------------------: ");

				// data JsonArray
				JSONObject mapJsonObj = new JSONObject(dmpLogMap.get(reducerMapKey.toString()).toString());
				
				for (EnumDataKeyInfo enumDataKeyInfo : EnumDataKeyInfo.values()) {
					String dataInfoStr = enumDataKeyInfo.toString();
					String dataInfoValue = (dataOrg.getJSONObject(dataInfoStr)).getString("value");
					boolean matchFlag = false;
					// 如果raw data的值不是null
					if (!StringUtils.equals(dataInfoValue, "null")) {
						JSONArray dataJsonAry = ((JSONObject) mapJsonObj.get("data")).getJSONArray(dataInfoStr);
						for (int i = 0; i < dataJsonAry.length(); i++) {
							JSONObject obj = dataJsonAry.getJSONObject(i);
							String value = obj.getString("value");
							// 如果這次的姓別值在map已有資料，即day_coount加1
							if (StringUtils.equals(dataInfoValue, value)) {
								String source = obj.getString("source");
								int day_count = obj.getInt("day_count");
								day_count = day_count + 1;
								obj.put("day_count", day_count);
								dataJsonAry.remove(i);
								dataJsonAry.put(obj);
								dmpLogMap.put(reducerMapKey.toString(), mapJsonObj.toString());
//								System.out.println("get 111111 MAPPPPP---: " + dmpLogMap.get(reducerMapKey.toString()));
								matchFlag = true;
								break;
							}
						}
						// 如果raw data的值在map中找不到，即新增一筆sex資料
						if (!matchFlag) {
							String dataInfoSourceOrg = (dataOrg.getJSONObject(dataInfoStr)).getString("source");
							String dataInfoValueOrg = (dataOrg.getJSONObject(dataInfoStr)).getString("value");
							JSONObject sexInfoData = new JSONObject();
							sexInfoData.put("source", dataInfoSourceOrg);
							sexInfoData.put("value", dataInfoValueOrg);
							sexInfoData.put("day_count", 1);
							dataJsonAry.put(sexInfoData);
							dmpLogMap.put(reducerMapKey.toString(), mapJsonObj.toString());
//							System.out.println("get 222222 MAPPPPP---: " + dmpLogMap.get(reducerMapKey.toString()));
						}
					}
				}
				
				// classify加總
				JSONArray mapClassifyAry = mapJsonObj.getJSONArray("classify");
				JSONArray classifyAryOrg = new JSONArray();
				classifyAryOrg = dataOrg.getJSONArray("classify");
				Map<String, Integer> classifyOrgMap = new HashMap<String, Integer>();
				// raw data的classify
				for (Object object : classifyAryOrg) {
					Iterator iterator = ((JSONObject) object).keys();
					while (iterator.hasNext()) {
						String key = (String) iterator.next();
						String value = ((JSONObject) object).getString(key);
						if (StringUtils.equals(value, "null")) {
							break;
						}
						String orgClassify = key + "_" + value;
						classifyOrgMap.put(orgClassify, 1);
					}
				}
				// System.out.println("classifyOrgMap : "+classifyOrgMap);

				// 比對map的classify，有吻合就加1
				for (Object mapObject : mapClassifyAry) {
					JSONObject mapObj = (JSONObject) mapObject;
					Iterator mapIterator = mapObj.keys();
					while (mapIterator.hasNext()) {
						String mapKey = (String) mapIterator.next();
						int mapValue = mapObj.getInt(mapKey);
						if (classifyOrgMap.get(mapKey) != null) {
							mapObj.put(mapKey, mapValue + 1);
							break;
						}
					}
				}
				dmpLogMap.put(reducerMapKey.toString(), mapJsonObj.toString());
//				System.out.println("0626-dmpLogMap : " + dmpLogMap.get(reducerMapKey.toString()).toString());
			}
			
//			log.info(">>>>>>reduce write key:" + sendKafkaJson.toString());
			
			
			
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " +e);
		}

	}

	public void cleanup(Context context) {
		try {
			Iterator iterator = dmpLogMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry mapEntry = (Map.Entry) iterator.next();
				producer.send(new ProducerRecord<String, String>("dmp_log_prd", "", mapEntry.getValue().toString()));
				
//				keyOut.set(mapEntry.getValue().toString());
//				context.write(keyOut, valueOut);
//				log.info(">>>>>>reduce Map send kafka:" + mapEntry.getValue().toString());
			}
			producer.close();

		} catch (Throwable e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}

	
//	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//}

//	
}
