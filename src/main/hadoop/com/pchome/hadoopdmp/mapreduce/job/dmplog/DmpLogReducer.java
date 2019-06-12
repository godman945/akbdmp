package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.pchome.hadoopdmp.mapreduce.job.component.PersonalInfoComponent;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper.combinedValue;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodborg.MongodbOrgHadoopConfig;
import com.pchome.soft.util.MysqlUtil;

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
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String[] weeks = {"sun","mon","tue","wed","thu","fri","sat"};
	private static Calendar calendar = Calendar.getInstance();
	private static String partitionHashcode = "1";
	private static int partition = 0;
	private static int total = 0;
	private static StringBuffer wiriteToDruid = new StringBuffer();
	private static JSONObject dmpJsonObj = null;
	private static JSONObject dmpJsonDataObj = null;
	private static net.minidev.json.JSONObject dmpJSon =  new net.minidev.json.JSONObject();
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	private static DBCollection dBCollection_user_detail;
	private DB mongoOrgOperations;
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();
	public static Map<String, String> pfbxWebsiteCategory = new HashMap<String, String>();
	private static Iterator<Row> rowIterator = null;
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
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
			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			dBCollection_user_detail = this.mongoOrgOperations.getCollection("user_detail");

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

			//load 推估分類個資表(ClsfyGndAgeCrspTable.txt)
			Configuration conf = context.getConfiguration();
			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
			Path clsfyTable = Paths.get(path[1].toString());
			Charset charset = Charset.forName("UTF-8");
			List<String> lines = Files.readAllLines(clsfyTable, charset);
			for (String line : lines) {
				String[] tmpStrAry = line.split(";"); // 0001000000000000;M,35
				String[] tmpStrAry2 = tmpStrAry[1].split(",");
				clsfyCraspMap.put(tmpStrAry[0],new combinedValue(tmpStrAry[1].split(",")[0], tmpStrAry2.length > 1 ? tmpStrAry2[1] : ""));
			}
			
			
			//取得DB所有網站分類代號
			MysqlUtil mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(context.getConfiguration().get("spring.profiles.active"));
			StringBuffer sql = new StringBuffer();
			sql.append(" SELECT a.customer_info_id,a.category_code FROM pfbx_allow_url a WHERE 1 = 1 and a.default_type = 'Y' ORDER BY a.customer_info_id  ");
			ResultSet resultSet = mysqlUtil.query(sql.toString());
			while(resultSet.next()){
				pfbxWebsiteCategory.put(resultSet.getString("customer_info_id"), resultSet.getString("category_code"));
			}
			mysqlUtil.closeConnection();
			
			//24館別階層對應表
			FileSystem fs = FileSystem.get(conf);
			org.apache.hadoop.fs.Path category24MappingFile = new org.apache.hadoop.fs.Path("/home/webuser/dmp/jobfile/24h_menu-1.xls");
			FSDataInputStream inputStream = fs.open(category24MappingFile);
			Workbook workbook = WorkbookFactory.create(inputStream);
			DataFormatter dataFormatter = new DataFormatter();
			Sheet sheet = workbook.getSheetAt(0);
			this.rowIterator = sheet.rowIterator();

//			inputStream.close();
//			fs.close();
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
//			log.info(">>>>>>>>>>>dmpJSon:"+dmpJSon);
//			log.info(">>>>>>>>>>>mapperKey:"+mapperKey.toString());
			int procsee = 0;
			for (Text text : mapperValue) {
				wiriteToDruid.setLength(0);
				dmpJSon.clear();
				dmpJSon = (net.minidev.json.JSONObject) jsonParser.parse(text.toString());
				if(StringUtils.isBlank(dmpJSon.getAsString("uuid"))) {
					log.error(">>>>>>>>>>>>>>>>>no uuid");
					break;
				}
				
				//6.個資
				try {
					if(procsee == 0) {
						personalInfoComponent.processPersonalInfo(dmpJSon, dBCollection_user_detail);
						procsee = procsee + 1;
					}
				}catch(Exception e) {
					log.error(">>>>>>>fail process processPersonalInfo:"+e.getMessage());
					continue;
				}
				//7.館別階層
				try {
					if(StringUtils.isNotBlank(dmpJSon.getAsString("op1"))) {
						log.info(">>>>>>>>>>>>1 op1:"+dmpJSon.getAsString("op1"));
						process24CategoryLevel(dmpJSon);
					}
				}catch(Exception e) {
					log.error(">>>>>>>fail process 24 category level:"+e.getMessage());
					continue;
				}
				
				
				
				
//				log.info(dmpJSon);
				wiriteToDruid.append("\""+dmpJSon.getAsString("uuid").toString()+"\"".trim());
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("hour")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("memid")).append("\"");
//				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid_flag")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("referer")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("url")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("domain")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_source")).append("\"");
				//kdcl格式資料
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfd_customer_info_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfp_customer_info_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("style_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("action_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("group_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_customer_info_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_position_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
				//pacl格式資料
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_x")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_y")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_event")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("event_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op1")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op2")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("email")).append("\"");
				//dmp資料
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex_source")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age_source")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("category")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("class_adclick_classify")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("category_source")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("user_agent")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_phone_info")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_os_info")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_browser_info")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info_source")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info_classify")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("time_info_source")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("time_info_classify")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ip")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_country")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_city")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_info_source")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_info_classify")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("trigger_type")).append("\"");
				calendar.setTime(sdf.parse(dmpJSon.getAsString("log_date")));
				int week_index = calendar.get(Calendar.DAY_OF_WEEK) - 1;
				if(week_index<0){
					week_index = 0;
				} 
				wiriteToDruid.append(",").append("\"").append(weeks[week_index]).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_ck")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_pv")).append("\"");
				if(StringUtils.isNotBlank(dmpJSon.getAsString("pfbx_customer_info_id"))) {
					wiriteToDruid.append(",").append("\"").append(pfbxWebsiteCategory.get(dmpJSon.getAsString("pfbx_customer_info_id"))).append("\"");
				}else {
					wiriteToDruid.append(",").append("\"").append("").append("\"");
				}
				//產出csv
				keyOut.set("\""+dmpJSon.getAsString("uuid")+"\"".trim());
				context.write(new Text(wiriteToDruid.toString()), null);
			}
			dmpJSon.clear();
			wiriteToDruid.setLength(0);
			
//			// log.info(">>>>>> reduce start : " + mapperKey.toString());
//			String data = mapperKey.toString();
//			JSONObject jsonObjOrg = (net.minidev.json.JSONObject) jsonParser.parse(data);
//
//			// String dmpSource = (String) jsonObjOrg.get("org_source");
//			String dmpMemid = (String) ((JSONObject) jsonObjOrg.get("key")).get("memid");
//			String dmpUuid = (String) ((JSONObject) jsonObjOrg.get("key")).get("uuid");
//			String recordDate = jsonObjOrg.getAsString("record_date");
//			record_date = (String) ((JSONObject) jsonObjOrg.get("key")).get("recordDate");
//			// 建立map key
//			// StringBuffer reducerMapKey = new StringBuffer();
//			// reducerMapKey.append(dmpSource);
//			// reducerMapKey.append("_");
//			// reducerMapKey.append(dmpMemid);
//			// reducerMapKey.append("_");
//			// reducerMapKey.append(dmpUuid);
//
//			// JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
//			if (((StringUtils.isNotBlank(dmpMemid) && !dmpMemid.equals("null"))
//					&& ((StringUtils.isNotBlank(dmpUuid) && !dmpUuid.equals("null"))))) {
//				// 先處理memid
//				StringBuffer reducerMapKey = new StringBuffer();
//				reducerMapKey.append(dmpMemid);
//				
////				log.info("kafkaDmpMap:"+kafkaDmpMap.get(reducerMapKey.toString()));
//				
//				JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
//				if (dmpJson == null) {
//					processKafakDmpMapKeyNotExist(recordDate, jsonObjOrg, reducerMapKey.toString());
//				} else {
//					processKafakDmpMapKeyIsExist(recordDate, jsonObjOrg, reducerMapKey.toString(), dmpJson);
//				}
//
//				JSONObject dmpUuidJson = kafkaDmpMap.get(dmpUuid);
//				if (dmpUuidJson == null) {
////					log.info(">>>>>>>>>3");
//					dmpUuidJson = (JSONObject) kafkaDmpMap.get(dmpMemid).clone();
//					JSONObject keyObject = (JSONObject) dmpUuidJson.get("key");
//					keyObject.put("uuid", dmpUuid);
//					keyObject.put("memid", "null");
//				} else {
////					log.info(">>>>>>>>>4");
//					dmpUuidJson = (JSONObject) kafkaDmpMap.get(dmpMemid).clone();
//					JSONObject keyObject = (JSONObject) dmpUuidJson.get("key");
//					keyObject.put("uuid", dmpUuid);
//					keyObject.put("memid", "null");
//					processKafakDmpMapKeyIsExist(recordDate, jsonObjOrg, dmpUuid.toString(), dmpUuidJson);
//				}
//			} else {
//				StringBuffer reducerMapKey = new StringBuffer();
//				if (((StringUtils.isNotBlank(dmpMemid) && !dmpMemid.equals("null")))) {
////					log.info(">>>>>>>>>7");
//					reducerMapKey.append(dmpMemid);
//				}
//				if (((StringUtils.isNotBlank(dmpUuid) && !dmpUuid.equals("null")))) {
////					log.info(">>>>>>>>>8");
//					reducerMapKey.append(dmpUuid);
//				}
//
//				JSONObject dmpJson = kafkaDmpMap.get(reducerMapKey.toString());
//				if (dmpJson == null) {
////					log.info(">>>>>>>>>9");
//					processKafakDmpMapKeyNotExist(recordDate, jsonObjOrg, reducerMapKey.toString());
//				} else {
////					log.info(">>>>>>>>>10");
//					processKafakDmpMapKeyIsExist(recordDate, jsonObjOrg, reducerMapKey.toString(), dmpJson);
//				}
//			}
// 
//			// if(dmpJson == null){
//			// processKafakDmpMapKeyNotExist();
//			//
//			// }else{
//			// processKafakDmpMapKeyIsExist();
//			// }
		} catch (Throwable e) {
			 log.error(">>>>>> reduce error :"+e.getMessage());
//			log.error("reduce error>>>>>> " + e);
//			// log.error(">>>>>>reduce error>> redisClassifyMap:" +
//			// redisClassifyMap);
		}
	}

	//處理24館別階層
	private void process24CategoryLevel(net.minidev.json.JSONObject dmpJSon) throws Exception{
        String op1 = dmpJSon.getAsString("op1");
		int level = 0;
        if(op1.length() == 4) {
        	level = 2;
		}
        if(op1.length() == 6) {
        	level = 3;
		}
//        log.info(">>>>>level:"+level);
        int alex = 0;
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
//            log.info("TEST>>>>>>>>>START");
//            log.info("TEST>>>>>>>>>level-1:"+row.getCell(1));
//        	log.info("TEST>>>>>>>>>level-2:"+row.getCell(3));
//        	log.info("TEST>>>>>>>>>level-3:"+row.getCell(5));
           
            
            if(level == 2) {
            	 if(alex == 0) {
                     log.info("TEST>>>>>>>>>START");
                     log.info("TEST>>>>>>>>>level-1:"+row.getCell(1));
                     log.info("TEST>>>>>>>>>level-2:"+row.getCell(3));
                     log.info("TEST>>>>>>>>>level-3:"+row.getCell(5));
                     alex = alex + 1;
            	 }
            	 log.info(">>>>>>>>>op1:"+op1+"["+row.getCell(3)+"]");
            }
            
            
            if(level == 2 && row.getCell(3).equals(op1)) {
            	log.info(">>>>>>>>>op1:"+op1);
            	log.info(">>>>>>>>>level-1:"+row.getCell(1));
            	log.info(">>>>>>>>>level-2:"+row.getCell(3));
            	log.info(">>>>>>>>>level-3:"+row.getCell(5));
            	break;
            }else if(level == 3 && row.getCell(5).equals(op1)) {
//            	log.info(">>>>>>>>>op1:"+op1);
//            	log.info(">>>>>>>>>level-1:"+row.getCell(1));
//            	log.info(">>>>>>>>>level-2:"+row.getCell(3));
//            	log.info(">>>>>>>>>level-3:"+row.getCell(5));
            	break;
            }
        }
	}
	
	
	
	// 處理mdp map不存在時
	private void processKafakDmpMapKeyNotExist(String recordDate, JSONObject jsonObjOrg, String reducerMapKey)
			throws Exception {
		// 處理info資訊
//		log.info("processKafakDmpMapKeyNotExist >>>>>>>> 1");
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
//		log.info("processKafakDmpMapKeyNotExist >>>>>>>> 2");
	}

	// 處理mdp map存在時
	private void processKafakDmpMapKeyIsExist(String recordDate, JSONObject jsonObjOrg, String reducerMapKey,
			JSONObject dmpJson) throws Exception {
//		log.info("processKafakDmpMapKeyIsExist >>>>>>>> 1");
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
//		log.info("processKafakDmpMapKeyIsExist >>>>>>>> 2");
	}
	
	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
	
	public void cleanup(Context context) {
		try {
//			String kafkaTopic;
//			String env = context.getConfiguration().get("spring.profiles.active");
//			if (env.equals("prd")) {
//				kafkaTopic = "dmp_log_prd";
//			} else {
//				kafkaTopic = "dmp_log_stg";
//			}
//			Iterator iterator = kafkaDmpMap.entrySet().iterator();
//			/*
//			 * druid [0]:date_time
//			 * druid [1]:date
//			 * druid [2]:time
//			 * druid [3]:uuid
//			 * druid [4]:category
//			 * druid [5]:user_agent
//			 * druid [6]:sex
//			 * druid [7]:age
//			 * druid [8]:area_country
//			 * druid [9]:area_city
//			 * druid [10]:device
//			 * druid [11]:device_os
//			 * druid [12]:device_browser
//			 * druid [13]:device_phone
//			 * druid [14]:url
//			 * druid [15]:ip
//			 * */
//			while (iterator.hasNext()) {
//				count = count + 1;
//				Map.Entry mapEntry = (Map.Entry) iterator.next();
//				
//				if(count == 1) {
//					log.info(">>>:"+mapEntry.getValue());
//				}
//				dmpJsonObj = (JSONObject)mapEntry.getValue();
//				dmpJsonDataObj = (JSONObject) dmpJsonObj.get("data");
////					log.info("mapEntry:"+mapEntry);
////					log.info("mapEntry size:"+kafkaDmpMap.size());
//					keyOut.set(((JSONObject)mapEntry.getValue()).getAsString("date_time"));
//					wiriteToDruid.append(",").append("\"").append(dmpJsonObj.getAsString("record_date")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("time_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(mapEntry.getKey().toString()).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("category_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJsonObj.get("user_agent")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("sex_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("age_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("area_country_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("area_city_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("device_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("device_os_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("device_browser_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(((JSONObject)((JSONArray)dmpJsonDataObj.get("device_phone_info")).get(0)).get("value")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJsonObj.get("url")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJsonObj.get("ip")).append("\"");
//					if(count <= 10) {
//						log.info(wiriteToDruid.toString());
//					}
//					
////					JSONArray arr =  (JSONArray) ((JSONObject)((JSONObject)mapEntry.getValue()).get("data")).get("classify");
////					for (Object object : arr) {
////						JSONObject ob = (JSONObject) object;
////						for(Iterator iterator2 = ob.keySet().iterator(); iterator2.hasNext();) {
////							  String key = (String) iterator2.next();
////							  log.info(key);
////							  log.info(ob.get(key));
////							  wiriteToDruid.append(",").append(ob.get(key));
////						}
////					}
////					log.info("--------");
//					context.write(keyOut, new Text(wiriteToDruid.toString()));
//					wiriteToDruid.setLength(0);
//				producer.send(new ProducerRecord<String, String>(kafkaTopic, partitionHashcode, mapEntry.getValue().toString()));
//				if(partition == 2){
//					partition = 0;
//					partitionHashcode = "1";
//				}else{
//					partition = partition + 1;
//					if(partition == 1){
//						partitionHashcode = "key0";
//					}
//					if(partition == 2){
//						partitionHashcode = "key2";
//					}
//				}
//			}
//			log.info("process count:"+count);
//			producer.close();
//			log.info(">>>>>>reduce count:" + count);
//			log.info(">>>>>>write clssify to Redis>>>>>");
//			log.info(">>>>>>cleanup redisClassifyMap:" + redisClassifyMap);
//			for (Entry<String, Integer> redisMap : redisClassifyMap.entrySet()) {
//				String redisKey = redisMap.getKey();
//				total = 0;
//				total = redisMap.getValue();
//				redisTemplate.opsForValue().increment(redisKey, total);
//				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
//			}
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
