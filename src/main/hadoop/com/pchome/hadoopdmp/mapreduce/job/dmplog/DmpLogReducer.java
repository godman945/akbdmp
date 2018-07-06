package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
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
	
	public Map<String,JSONObject> kafkaDmpMap =null;
	
	public Map<String,Integer> redisClassifyMap =null;
	
	
	/***/
	private DB db;
	private List<BasicDBObject> queryObj = null;
	private BasicDBObject andQuery = null;
	private DBCollection dBCollection = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private BasicDBObject updateDmpDocuments = new BasicDBObject();
	private BasicDBObject createDocuments = null;
	private List<String> memidList = null;
	private BasicDBObject basicDBObjec = null;
	private DBObject updateSetValue = new BasicDBObject();
	private List<BasicDBObject> obj = null;
	private int time = 0;
	/***/
	
	
	
	
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
			redisClassifyMap = new HashMap<String,Integer>();
			for (EnumClassifyKeyInfo enumClassifyKeyInfo : EnumClassifyKeyInfo.values()) {
				redisClassifyMap.put(redisFountKey + enumClassifyKeyInfo.toString(), 0);
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
						} else{	
							//type值是Y或N
							key = key +"_"+type;
						} 
						int classifyValue = redisClassifyMap.get(key);
						classifyValue = classifyValue + 1;
						redisClassifyMap.put(key, classifyValue);
					}
				}
				kafkaDmpMap.put(reducerMapKey.toString(), jsonObjOrg);
				
			}else{
				JSONObject hadoopDataOrg = ((JSONObject) jsonObjOrg.get("data"));
				JSONObject hadoopDataDmpMap = ((JSONObject) dmpJson.get("data"));
				for (EnumDataKeyInfo enumDataKeyInfo : EnumDataKeyInfo.values()) {
					String source = ((JSONObject) hadoopDataOrg.get(enumDataKeyInfo.toString())).getAsString("source");
					String value = ((JSONObject) hadoopDataOrg.get(enumDataKeyInfo.toString())).getAsString("value");
					// 此次log資訊來源及值都不為null才取出資料進行判斷是否加1邏輯
					if ( (StringUtils.isNotBlank(source) && !source.equals("null"))
							&& (StringUtils.isNotBlank(value) && !value.equals("null")) ) {
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
							array.add(infoJson);
						}
					}
				}

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
						} else{
							//type值是Y或N
							key = key +"_"+type;
						} 
						int classifyValue = redisClassifyMap.get(key);
						classifyValue = classifyValue + 1;
						redisClassifyMap.put(key, classifyValue);
					}
				}
				kafkaDmpMap.put(reducerMapKey.toString(), dmpJson);
			}
		} catch (Throwable e) {
//			log.error(">>>>>> reduce error redis key:" +reducerMapKey.toString());
			log.error("reduce error>>>>>> " +e);
			log.error(">>>>>>reduce error>> redisClassifyMap:" + redisClassifyMap);
		}
	}
	
	public void cleanup(Context context) {
		try {
			
			this.jsonParser =  new JSONParser(JSONParser.MODE_PERMISSIVE);
			this.queryObj = new ArrayList<BasicDBObject>();
			this.andQuery = new BasicDBObject();
			this.createDocuments = new BasicDBObject();
			this.memidList = new ArrayList<String>();
			this.basicDBObjec = new BasicDBObject();
			this.obj = new ArrayList<BasicDBObject>();
			Mongo mongo; 
			mongo = new Mongo("mongodb.mypchome.com.tw");
			this.db = mongo.getDB("dmp");
			this.db.authenticate("webuser", "MonG0Dmp".toCharArray());
			this.dBCollection = db.getCollection("user_detail");
			
			
			
//			log.info(">>>>>>write cleanup>>>>>");
			String kafkaTopic;
			String env = context.getConfiguration().get("spring.profiles.active");
			if(env.equals("prd")){
				kafkaTopic = "dmp_log_prd";
			}else{
				kafkaTopic = "dmp_log_stg";
			}
			log.info(">>>>>>kafkaTopic: " + kafkaTopic);
			
			Iterator iterator = kafkaDmpMap.entrySet().iterator();
			while (iterator.hasNext()) {
				count = count+1;
				Map.Entry mapEntry = (Map.Entry) iterator.next();
//				Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>(kafkaTopic, "", mapEntry.getValue().toString()));
//				while (!f.isDone()) {
//				}
//				keyOut.set(mapEntry.getValue().toString());
//				context.write(keyOut, valueOut);
//				log.info(">>>>>>reduce Map send kafka:" + mapEntry.getValue().toString());
				
				
				String tupleStr = mapEntry.getValue().toString();
				JSONObject json = (JSONObject) this.jsonParser.parse(tupleStr);
				String memid = "";
				String uuid = "";
				JSONObject key = (JSONObject) json.get("key");
				if(key != null){
					memid = key.getAsString("memid");
					uuid = key.getAsString("uuid");
				}
				if((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null")) ){
					log.info("memid and uuid is null");
					log.info(tupleStr);
					continue;
				}
				if(StringUtils.isNotBlank(memid) && !memid.equals("null")){
					process(json,memid,"memid");
				}
				if(StringUtils.isNotBlank(uuid) && !uuid.equals("null")){
					process(json,uuid,"uuid");
				}
				
				
				
				
				
				
			}
			producer.close();
			log.info(">>>>>>reduce count:" + count);
			
			log.info(">>>>>>write clssify to Redis>>>>>");
			log.info(">>>>>>cleanup redisClassifyMap:" + redisClassifyMap);
			for (Entry<String, Integer> redisMap : redisClassifyMap.entrySet()) {
				String redisKey = redisMap.getKey();
				int count = redisMap.getValue();
				redisTemplate.opsForValue().increment(redisKey, count);
				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
			}
			
		} catch (Throwable e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/******************/
	/**
	 * 處理kafka送入DMP資料 1.無資料則新增 2.uuid有對應的memid同步更新該筆memid
	 * @param dmpJson dmp原始資料
	 * @param userId dmp傳入需記錄的uuid,memid
	 * @param type 處理類型為uuid或是memid
	 * 
	 * */
	@SuppressWarnings("unchecked")
	public void process(JSONObject dmpJson,String userId,String type) throws Exception {
		String memid = ((JSONObject)dmpJson.get("key")).getAsString("memid");
		String uuid = ((JSONObject)dmpJson.get("key")).getAsString("uuid");
		if(memid.equals("null")){
			memid = "";
		}
		if(uuid.equals("null")){
			uuid = "";
		}
		String mSex = "";
		String mAge = "";
		//類型為uuid時撈取查看是否有對應memid需全部更新
		JSONObject hadoopData =  ((JSONObject)dmpJson.get("data"));
		String recodeDate = hadoopData.getAsString("record_date");
		Set<Entry<String, Object>> hadoopJsonDataSet = (hadoopData).entrySet();
		//1.Log中memid與uuid都存在 傳入類型為uuid且memid不為空
		if(type.equals("uuid") && StringUtils.isNotBlank(memid) && StringUtils.isNotBlank(uuid)){
			DBObject dbObject = queryUserDetail(memid); 
			if(dbObject != null){
				mSex = ((DBObject)dbObject.get("user_info")).get("msex") == null ? "" : ((DBObject)dbObject.get("user_info")).get("msex").toString();
				mAge = ((DBObject)dbObject.get("user_info")).get("mage") == null ? "" : ((DBObject)dbObject.get("user_info")).get("mage").toString();
			}
			DBObject dbUuidObject = queryUserDetail(userId);
			if(dbUuidObject == null){
				createDocuments.clear();
				BasicDBObject createDocuments = this.createDocuments;
				createDocuments.append("user_id", userId);
				createDocuments.append("create_date", recodeDate);
				createDocuments.append("update_date", recodeDate);
				BasicDBObject basicDBObjec = new BasicDBObject();
				basicDBObjec.append("type", type);
				memidList.clear();
				List<String> memidList = this.memidList;
				memidList.add(memid);
				basicDBObjec.append("memid", memidList);
				createDocuments.append("user_info", dbObject.get("user_info"));
				((BasicDBObject)createDocuments.get("user_info")).append("memid", memidList);
				createDocuments.append("category_info", dbObject.get("category_info"));
				dBCollection.insert(createDocuments);
			}else{
				//查詢uuid對應的全部memid進行更新
				andQuery.clear();
				BasicDBObject andQuery = this.andQuery;
				obj.clear();
				List<BasicDBObject> obj = this.obj;
				obj.add(new BasicDBObject("user_info.type", "uuid"));
				obj.add(new BasicDBObject("user_info.memid", memid));
				andQuery.put("$and", obj);
				DBCursor dbCursor = (DBCursor) dBCollection.find(andQuery);
				while (dbCursor.hasNext()) {
					DBObject dbObj = dbCursor.next();
					String uuidMappingMemid = (String) dbObj.get("user_id");
					writeHadoopToMongo(dbUuidObject,uuidMappingMemid,recodeDate,"uuid",hadoopJsonDataSet);
				}
			}
		}else{
			long time1  = System.currentTimeMillis();
			andQuery.clear();
			BasicDBObject andQuery = this.andQuery;
			obj.clear();
			List<BasicDBObject> obj = this.obj;
			obj.add(new BasicDBObject("user_info.type", type));
			obj.add(new BasicDBObject("user_id", userId));
			andQuery.put("$and", obj);
			
			DBCursor dbCursor =  (DBCursor) dBCollection.find(andQuery);
			
			if(dbCursor.count() == 0){
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
				createDocuments.clear();
				BasicDBObject createDocuments = this.createDocuments;
				createDocuments.append("user_id", userId);
				createDocuments.append("create_date", recodeDate);
				createDocuments.append("update_date", recodeDate);
				createDocuments.append("category_info", list);
				
				basicDBObjec.clear();
				BasicDBObject basicDBObjec = this.basicDBObjec;
				basicDBObjec.append("type", type);
				basicDBObjec.append("memid", new ArrayList<String>());
				basicDBObjec.append("msex", "");
				basicDBObjec.append("mage", "");
				basicDBObjec.append("sex_info", list);
				basicDBObjec.append("device_phone_info", list);
				basicDBObjec.append("device_browser_info", list);
				basicDBObjec.append("device_os_info", list);
				basicDBObjec.append("age_info", list);
				basicDBObjec.append("area_country_info", list);
				basicDBObjec.append("device_info", list);
				basicDBObjec.append("area_city_info", list);
				basicDBObjec.append("time_info", list);
				createDocuments.append("user_info", basicDBObjec);
				dBCollection.insert(createDocuments);
				DBObject dbObject = queryUserDetail(userId);
				writeHadoopToMongo(dbObject,userId,recodeDate,type,hadoopJsonDataSet);
				long time2  = System.currentTimeMillis();
				log.info("total update-1 count:"+time+" cost:"+(time2-time1)+"ms");
			}else{
				time1  = System.currentTimeMillis();
				while (dbCursor.hasNext()) {
					DBObject dbObj = dbCursor.next();
					String uuidMappingMemid = (String) dbObj.get("user_id");
					mSex = ((DBObject)dbObj.get("user_info")).get("msex") == null ? "" : ((DBObject)dbObj.get("user_info")).get("msex").toString();
					mAge = ((DBObject)dbObj.get("user_info")).get("mage") == null ? "" : ((DBObject)dbObj.get("user_info")).get("mage").toString();
					writeHadoopToMongo(dbObj,uuidMappingMemid,recodeDate,type,hadoopJsonDataSet);
					if(type.equals("uuid")){
						List<String> memidList = null;
						if(((DBObject) dbObj.get("user_info")).get("memid") instanceof String){
							memidList = new ArrayList<String>();
							String memidStr = (String) ((DBObject) dbObj.get("user_info")).get("memid");
							if(memidStr.length() > 0){
								memidList.add(memidStr);
							}
						}else{
							memidList =  (List<String>) ((DBObject) dbObj.get("user_info")).get("memid");	
						}
						if(memidList != null && !memidList.isEmpty() || memidList.size() > 0){
							for (String memidStr : memidList) {
								DBObject dbObject = queryUserDetail(memidStr);
								if(dbObject != null){
									writeHadoopToMongo(dbObject,memidStr,recodeDate,"memid",hadoopJsonDataSet);
								}
							}
						}
					}
				}
				long time2  = System.currentTimeMillis();
				log.info("total update-2 count:"+time+" cost:"+(time2-time1)+"ms");
			}
		}
	}
	
	/*
	 * 第一次預設權重
	 * */
	private double getDefaultW(){
		double pExpv = Math.exp(-1 * 0.05);
		return (1 / (1 + pExpv));
	}
	
	/*
	 * 計算權重 
	 * @param w 目前權重
	 * @param count 此次需調升次數
	 * */
	public double getPower(double w,int count){
		double nw = w;
		for (int i = 0; i < count; i++) {
			double pExpv = Math.exp(-1 * 0.05);
			nw = nw + (1 / (1 + pExpv));
		}
		return nw;
	}
	
	/**
	 * 新來源加入(不重複)
	 * */
	public List<String> getSource(String source,List<String> sourceList){
		Set<String> set = new HashSet<String>();
		List<String> newSourceList = sourceList;
		newSourceList.add(source);
		set.addAll(newSourceList);
		sourceList.clear();
		sourceList = new ArrayList<String>(set);
		return sourceList;
	}
	
	/**
	 * 此小時若有提升的DMP INFO該DMP其他值需要降溫，一天只降溫一次
	 * */
	public List<Map<String,Object>> processDown(Set<String> upSet,List<Map<String,Object>> dmpList,String recodeDate,String mongoPropertyKey) throws Exception{
		for (String upStr : upSet) {
			for (Map<String, Object> map : dmpList) {
				String value = (String) map.get(mongoPropertyKey);
				String updateDate = "";
				if(map.get("update_date") == null){
					updateDate = recodeDate;
				}else{
					updateDate = (String) map.get("update_date");
				}
				Double w = null;
				if(map.get("w") instanceof Integer){
					w = getDefaultW();
					map.put("w",w);
				}
				w = (Double) map.get("w");
				if(value.equals(upStr) && !recodeDate.equals(updateDate)){
					Date startDate = sdf.parse(updateDate);
					Date endDate = sdf.parse(recodeDate);
					int betweenDate = (int) ((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
					if(betweenDate > 0){
						double nw = w * Math.exp(-0.1 * (betweenDate * 0.1));
//						牛頓冷卻 到負值 ，直接給0
						if (nw <= 0) {
							nw = 0;
						}
						map.put("w", nw);
						map.put("update_date", recodeDate);
					}
				}
			}
		}
		return dmpList;
	}
	
	
	/**
	 * @param mongoUserInfo mongo中user_info資料
	 * @param mongoInfoOfKey mongo中user_info底下dmp key名稱
	 * @param mongoPropertyKey dmp物件中key名稱
	 * @param dmpValue hadoop傳送的dmp key值
	 * @param source hadoop傳送的dmp來源
	 * @param recodeDate hadoop傳送的記錄日
	 * 牛頓冷卻 新權重 = w * Math.exp(-0.1 * (1 * 0.1)); 新權重 = 上一次的權重 * Math.exp(-0.1 *
	 * (天*0.1))
	 * 
	 * 邏輯迴歸線性增加公式 double pExpv = 0; pExpv = Math.exp(-1 * 0.05); new_w = w + (1
	 * / (1 + pExpv)); 新權重 = 上一次的權重 + (1 / (1 + pExpv));
	 * 
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,Object>> processDmpInfo(DBObject dbObject,String mongoInfoOfKey,String mongoPropertyKey,String recodeDate,BasicDBObject updateDocuments,JSONArray array,List<Map<String,Object>> dmpList) throws Exception{
		if(array != null && array.size() > 0){
			String dayCountKey = "day_count";
			String sourceKey = "source";
			String valueKey = "value";
			String userDayCountKey ="user_info_day_count";
			String userMillionCountKey ="user_info_million_count";
			if(mongoInfoOfKey.equals("category_info")){
				userMillionCountKey = "ad_class_million_count";
				userDayCountKey = "ad_class_day_count";
			}
			Set<String> downSet = new HashSet<String>();
			for (int i = 0;i < array.size(); i++) {
				JSONObject dmpJson = (JSONObject) array.get(i);
				int count = 0;
				if(dmpJson.containsKey(dayCountKey)){
					count = (Integer) dmpJson.get(dayCountKey);
				}	
				String source2 = dmpJson.getAsString(sourceKey);
				String value2 = dmpJson.getAsString(valueKey);
				//處理存在的key提高權重
				if(count > 0 && dmpList != null){
					boolean flag = false;
					for (Map<String, Object> map : dmpList) {
						String mongoValue = (String)map.get(mongoPropertyKey);
						if(value2.equals(mongoValue)){
							flag = true;
							if(map.get(userDayCountKey) == null){
								map.put(userDayCountKey, 1);
							}
							if(map.get(userMillionCountKey) == null){
								map.put(userMillionCountKey, 0);
							}
							if(map.get("w") instanceof Integer){
								map.put("w", getDefaultW());
							}
							int dayCount = (Integer) map.get(userDayCountKey);
							int millionCount = (Integer) map.get(userMillionCountKey);
							dayCount = dayCount + count;
							if(dayCount > 10000000){
								millionCount = millionCount + 1;
								dayCount = dayCount - 10000000;
							}
							map.put(userDayCountKey,dayCount);
							map.put(userMillionCountKey,millionCount);
							double w = (Double) map.get("w");
							w = getPower(w,count);
							map.put("w", w);
							List<String> sourceList = (List<String>) map.get(sourceKey);
							List<String> newSourceList = getSource(source2,sourceList);
							map.put(mongoPropertyKey,value2);
							map.put(sourceKey,newSourceList);
							map.put("update_date",recodeDate);
						}else{
							downSet.add(mongoValue);
						}
					}
					if(!flag){
						Map<String, Object> map = new HashMap<String, Object>();
						map.put(userMillionCountKey,0);
						map.put(userDayCountKey,count);
						double w = getPower(getDefaultW(),count);
						map.put("w", w);
						map.put(mongoPropertyKey,value2);
						List<String> sourceList = new ArrayList<String>();
						sourceList.add(source2);
						map.put(sourceKey,sourceList);
						map.put("update_date",recodeDate);
						dmpList.add(map);
					}
				}else if((StringUtils.isNotBlank(source2) && !source2.equals("null")) && (StringUtils.isNotBlank(value2) && !value2.equals("null"))){
					Map<String, Object> map = new HashMap<String, Object>();
					map.put(userMillionCountKey,0);
					map.put(userDayCountKey,count);
					double w = getPower(getDefaultW(),count);
					map.put("w", w);
					map.put(mongoPropertyKey,value2);
					List<String> sourceList = new ArrayList<String>();
					sourceList.add(source2);
					map.put(sourceKey,sourceList);
					map.put("update_date",recodeDate);
					
					dmpList = new ArrayList<Map<String,Object>>();
					dmpList.add(map);
				}
				dmpList = processDown(downSet,dmpList,recodeDate,mongoPropertyKey);
			}
		}
		if(dmpList == null){
			return new ArrayList<Map<String,Object>>();
		}else{
			return dmpList;	
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public boolean writeHadoopToMongo(DBObject userDetailMongoBean,String userId,String recodeDate,String type,Set<Entry<String, Object>> hadoopJsonDataSet) throws Exception{
		long time1  =System.currentTimeMillis();
		updateDmpDocuments.clear();
		BasicDBObject dmpDocuments = (BasicDBObject) userDetailMongoBean.get("user_info");
		//更新對應的資料
		for (Entry<String, Object> entry : hadoopJsonDataSet) {
			String dmpDataKey = entry.getKey();
			Object dmpDataValue = entry.getValue();
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.CATEGORY_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get(AkbDmpLogInfoEnum.CATEGORY_INFO.getKey()));
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.CATEGORY_INFO.getKey(),AkbDmpLogInfoEnum.CATEGORY_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				updateDmpDocuments.append(AkbDmpLogInfoEnum.CATEGORY_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.SEX_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.SEX_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.SEX_INFO.getKey(),AkbDmpLogInfoEnum.SEX_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.SEX_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.AGE_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.AGE_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.AGE_INFO.getKey(),AkbDmpLogInfoEnum.AGE_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.AGE_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.DEVICE_PHONE_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.DEVICE_PHONE_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.DEVICE_PHONE_INFO.getKey(),AkbDmpLogInfoEnum.DEVICE_PHONE_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.DEVICE_PHONE_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.DEVICE_BROWSER_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.DEVICE_BROWSER_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.DEVICE_BROWSER_INFO.getKey(),AkbDmpLogInfoEnum.DEVICE_BROWSER_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.DEVICE_BROWSER_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.DEVICE_OS_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.DEVICE_OS_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.DEVICE_OS_INFO.getKey(),AkbDmpLogInfoEnum.DEVICE_OS_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.DEVICE_OS_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.DEVICE_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.DEVICE_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.DEVICE_INFO.getKey(),AkbDmpLogInfoEnum.DEVICE_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.DEVICE_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.AREA_COUNTRY_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.AREA_COUNTRY_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.AREA_COUNTRY_INFO.getKey(),AkbDmpLogInfoEnum.AREA_COUNTRY_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.AREA_COUNTRY_INFO.getKey(), list);
			}
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.AREA_CITY_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.AREA_CITY_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.AREA_CITY_INFO.getKey(),AkbDmpLogInfoEnum.AREA_CITY_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.AREA_CITY_INFO.getKey(), list);
			}
			
			if(dmpDataKey.equals(AkbDmpLogInfoEnum.TIME_INFO.getKey())){
				JSONArray array = (JSONArray) dmpDataValue;
				List<Map<String,Object>> list = (List<Map<String, Object>>) ((DBObject)userDetailMongoBean.get("user_info")).get(AkbDmpLogInfoEnum.TIME_INFO.getKey());
				list = processDmpInfo(userDetailMongoBean,AkbDmpLogInfoEnum.TIME_INFO.getKey(),AkbDmpLogInfoEnum.TIME_INFO.getInfoKey(),recodeDate,dmpDocuments,array,list);
				dmpDocuments.append(AkbDmpLogInfoEnum.TIME_INFO.getKey(), list);
			}
		}
		
		updateDmpDocuments.append("user_info", dmpDocuments);
		DBObject updateCondition = new BasicDBObject();
		updateCondition.put("user_id", userId);
		updateCondition.put("user_info.type", type);
		updateCondition.put("_id", userDetailMongoBean.get("_id"));
		processInfoWeight(updateDmpDocuments);
		updateSetValue.put("$set", updateDmpDocuments);
//		dBCollection.update(updateCondition, updateSetValue);
		long time2  =System.currentTimeMillis();
		
		log.info(">>>>>>>>>>do update:"+(time2-time1)+"ms");
		return true;
	}
	
	@SuppressWarnings("unchecked")
	public BasicDBObject processInfoWeight(BasicDBObject updateDmpDocuments) throws Exception{
		try{
			for (AkbDmpLogInfoEnum dmpLogInfoEnum : AkbDmpLogInfoEnum.values()) {
				if(AkbDmpLogInfoEnum.TIME_INFO == dmpLogInfoEnum || AkbDmpLogInfoEnum.CATEGORY_INFO == dmpLogInfoEnum){
					continue;
				}
				List<Map<String, Object>> mongoDmpInfo = (List<Map<String, Object>>) ((DBObject) updateDmpDocuments.get("user_info")).get(dmpLogInfoEnum.getKey());
				if(mongoDmpInfo != null){
					double wPrower = 0;
					String wValue = "";
					for (Map<String, Object> map : mongoDmpInfo) {
						if(map.get("w") instanceof Integer){
							map.put("w", getDefaultW());
						}
						double w = (Double) map.get("w");
						if(w > wPrower){
							wPrower = w;
							wValue = (String) map.get(dmpLogInfoEnum.getInfoKey());
						}
					}
					if(dmpLogInfoEnum.getKey().equals("age_info") || 
							dmpLogInfoEnum.getKey().equals("sex_info") ||
							dmpLogInfoEnum.getKey().equals("device_phone_info") || 
							dmpLogInfoEnum.getKey().equals("area_city_info") ||
							dmpLogInfoEnum.getKey().equals("device_browser_info") 
							){
						((BasicDBObject)updateDmpDocuments.get("user_info")).put(dmpLogInfoEnum.getInfoKey(), wValue);
					}
				}
			}
		}catch(Exception e){
			log.error("-----TEST--------");
			log.error(">>>>>>>>>>>>>>>"+updateDmpDocuments);
			return updateDmpDocuments;
		}
		
		return updateDmpDocuments;
	}
	
	
	public DBObject queryUserDetail(String memid) throws Exception {
		this.andQuery.clear();
		BasicDBObject andQuery = this.andQuery;
		this.queryObj.clear();
		List<BasicDBObject> obj = this.queryObj;
		obj.add(new BasicDBObject("user_id", memid));
		andQuery.put("$and", obj);
		return dBCollection.findOne(andQuery);
	}
	/******************/
	
	
	
	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		
//		DmpLogReducer dmpLogReducer = ctx.getBean(DmpLogReducer.class);
////		String a ="{'key':{'memid':'null','uuid':'c014b82c-65c3-4e59-88ac-825152412307'},'data':{'category_info':{'source':'null','value':'null'},'sex_info':{'source':'null','value':'null'},'age_info':{'source':'null','value':'null'},'area_country_info':{'source':'ip','value':'Taiwan'},'area_city_info':{'source':'ip','value':'Chengde'},'device_info':{'source':'user-agent','value':'MOBILE'},'device_phone_info':{'source':'user-agent','value':'APPLE'},'device_os_info':{'source':'user-agent','value':'IOS'},'device_browser_info':{'source':'user-agent','value':'Mobile Safari'},'time_info':{'source':'datetime','value':'17'},'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'Y'},{'all_kdcl_log_time_info':'Y'}]},'url':'','ip':'101.139.176.46','record_date':'2018-06-11','org_source':'kdcl','date_time':'2018-05-22 17:07:44','user_agent':'Mozilla\\5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) AppleWebKit\\604.1.34 (KHTML, like Gecko) GSA\\37.1.171590344 Mobile\\15E302 Safari\\604.1','ad_class':'0017024822530000','record_count':658}";
//		String a ="{'key':{'memid':'null','uuid':'c014b82c-65c3-4e59-88ac-825152412307'},'data':{'category_info':{'source':24h,'value':7997979979},'sex_info':{'source':'null','value':'null'},'age_info':{'source':'null','value':'null'},'area_country_info':{'source':'ip','value':'Taiwan'},'area_city_info':{'source':'ip','value':'Chengde'},'device_info':{'source':'user-agent','value':'MOBILE'},'device_phone_info':{'source':'user-agent','value':'APPLE'},'device_os_info':{'source':'user-agent','value':'IOS'},'device_browser_info':{'source':'user-agent','value':'Mobile Safari'},'time_info':{'source':'datetime','value':'17'},'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'Y'},{'all_kdcl_log_time_info':'Y'}]},'url':'http:\\\\www.gaomei.com.tw\\openhours\','ip':'101.139.176.46','record_date':'2018-06-11','org_source':'kdcl','date_time':'2018-05-22 17:05:53','user_agent':'Mozilla\\5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) AppleWebKit\\604.1.34 (KHTML, like Gecko) GSA\\37.1.171590344 Mobile\\15E302 Safari\\604.1','ad_class':'0017024822530000','record_count':287}";
//		JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
//		net.minidev.json.JSONObject json = (net.minidev.json.JSONObject) jsonParser.parse(a);
//		for (int i = 0; i < 1; i++) {
//			dmpLogReducer.reduce(new Text(json.toString()), null, null);
//		}	
		
		
//		RedisTemplate<String, Object> redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
//		System.out.println(redisTemplate.opsForValue().get("kdcl_null_d9228b87-bc6d-4e23-a4e6-0d7563380c7e"));
		
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
