package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.pchome.hadoopdmp.mapreduce.job.component.PersonalInfoComponent;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodborg.MongodbOrgHadoopConfig;
import com.pchome.soft.util.MysqlUtil;

import net.minidev.json.parser.JSONParser;

@SuppressWarnings("deprecation")
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

	public static int count_test = 0;

	public JSONParser jsonParser = null;

	public String redisFountKey;


	public Map<String, Integer> redisClassifyMap = null;
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String[] weeks = {"SUN","MON","TUE","WED","THU","FRI","SAT"};
	private static String[] markLevelList = {"mark_layer1","mark_layer2","mark_layer3"};
	private static String[] markValueList = {"mark_value1","mark_value2","mark_value3"};
	private static Calendar calendar = Calendar.getInstance();
	private static StringBuffer wiriteToDruid = new StringBuffer();
	private static net.minidev.json.JSONObject dmpJSon =  new net.minidev.json.JSONObject();
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	private static DBCollection dBCollection_user_detail;
	private DB mongoOrgOperations;
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();
	public static Map<String, String> pfbxWebsiteCategory = new HashMap<String, String>();
	public static List<String> categoryLevelMappingList = new ArrayList<String>();
	private static Map<String, String> existLevelCode = new HashMap<String, String>();
	
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
			
			log.info(">>>>>>>>>>>>>>>>>>>>categoryLevelMappingMap:"+DmpLogMapper.categoryLevelMappingMap);
			
			
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}
	
	private static int count = 0;
	private static Set<String> uuidSet = new HashSet<String>();
	
	private static Map<String,Integer> uuidMap = new HashMap<String,Integer>();
	
	
	
	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
////			log.info(">>>>>>>>>>>dmpJSon:"+dmpJSon);
////			log.info(">>>>>>>>>>>mapperKey:"+mapperKey.toString());
//			for (Text text : mapperValue) {
//				wiriteToDruid.setLength(0);
//				dmpJSon.clear();
//				dmpJSon = (net.minidev.json.JSONObject) jsonParser.parse(text.toString());
//				if(StringUtils.isBlank(dmpJSon.getAsString("uuid"))) {
//					log.error(">>>>>>>>>>>>>>>>>no uuid");
//					break;
//				}
//				
//				//6.個資
//				try {
//					personalInfoComponent.processPersonalInfo(dmpJSon, dBCollection_user_detail);
//				}catch(Exception e) {
//					log.error(">>>>>>>fail process processPersonalInfo:"+e.getMessage());
//					continue;
//				}
//				
//				calendar.setTime(sdf.parse(dmpJSon.getAsString("log_date")));
//				int week_index = calendar.get(Calendar.DAY_OF_WEEK) - 1;
//				if(week_index<0){
//					week_index = 0;
//				}
//				String pfbxCustomerInfoId = dmpJSon.getAsString("pfbx_customer_info_id");
//				String webClass = StringUtils.isBlank(pfbxWebsiteCategory.get(pfbxCustomerInfoId)) ? "" : pfbxWebsiteCategory.get(pfbxCustomerInfoId);
//				//產出csv
//				boolean flag = false;
//				for (int i= 0; i < markLevelList.length; i++) {
//					if(StringUtils.isNotBlank(dmpJSon.getAsString(markLevelList[i]))) {
//						flag = true;
//						//log.info(dmpJSon);
//						wiriteToDruid.append("\""+dmpJSon.getAsString("fileName")+"\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("time_info_source")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.get("memid")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid_flag")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ip")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("url")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("referer")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("domain")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_source")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("trigger_type")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfp_customer_info_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfd_customer_info_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_customer_info_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("style_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("action_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("group_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_position_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_country")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_city")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_phone_info")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_os_info")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_browser_info")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex_source")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age_source")).append("\"");
//						wiriteToDruid.append(",").append("\"").append("audicen_id_default").append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_x")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_y")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_event")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("event_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_id")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_price")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_dis")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString(markValueList[i])).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString(markLevelList[i])).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op1")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op2")).append("\"");
//						wiriteToDruid.append(",").append("\"").append("ad_price_default").append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("cks")).append("\"");
//						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pvs")).append("\"");
//						keyOut.set("\""+dmpJSon.getAsString("uuid")+"\"".trim());
//						context.write(new Text(wiriteToDruid.toString()), null);
//						wiriteToDruid.setLength(0);
//					}
//				}
//				
//				if(!flag) {
//					wiriteToDruid.append("\""+dmpJSon.getAsString("fileName")+"\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("time_info_source")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.get("memid")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid_flag")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ip")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("url")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("referer")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("domain")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_source")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("trigger_type")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfp_customer_info_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfd_customer_info_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_customer_info_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("style_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("action_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("group_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_position_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_country")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_city")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_phone_info")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_os_info")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_browser_info")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex_source")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age_source")).append("\"");
//					wiriteToDruid.append(",").append("\"").append("audicen_id_default").append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_x")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_y")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_event")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("event_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_id")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_price")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_dis")).append("\"");
//					wiriteToDruid.append(",").append("\"").append("").append("\"");
//					wiriteToDruid.append(",").append("\"").append("").append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op1")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op2")).append("\"");
//					wiriteToDruid.append(",").append("\"").append("ad_price_default").append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("cks")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pvs")).append("\"");
//					keyOut.set("\""+dmpJSon.getAsString("uuid")+"\"".trim());
//					context.write(new Text(wiriteToDruid.toString()), null);
//					wiriteToDruid.setLength(0);
//				}
////				keyOut.set("\""+dmpJSon.getAsString("uuid")+"\"".trim());
////				context.write(new Text(wiriteToDruid.toString()), null);
//				if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_layer3"))) {
//					log.info(">>>>>>>>>>>>>>> mark_layer3:"+dmpJSon);
//				}
//				if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_layer2"))) {
//					log.info(">>>>>>>>>>>>>>> mark_layer2:"+dmpJSon);
//				}
//				if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_layer1"))) {
//					log.info(">>>>>>>>>>>>>>> mark_layer1:"+dmpJSon);
//				}
//			}
//			
//			dmpJSon.clear();
//			wiriteToDruid.setLength(0);
			
			
			
//			log.info(">>>>>>>>>>>mapperKey:"+mapperKey.toString());
				uuidSet.clear();
				int pv = 0;
				for (Text text : mapperValue) {
					wiriteToDruid.setLength(0);
					dmpJSon.clear();
					dmpJSon = (net.minidev.json.JSONObject) jsonParser.parse(text.toString());
//					log.info(dmpJSon);
					if(StringUtils.isBlank(dmpJSon.getAsString("uuid"))) {
						break;
					}
					uuidSet.add(dmpJSon.getAsString("uuid"));
					pv = pv + 1;
				}
				uuidMap.put(mapperKey.toString(), uuidSet.size());
				uuidMap.put(mapperKey.toString()+"_PV", pv);
				
			
			
				
				
				
				
				
				
				
				
				
			
			
			
			
		} catch (Throwable e) {
			 log.error(">>>>>> reduce error :"+e.getMessage());
		}
	}
	
	
	public void cleanup(Context context) {
		try {
//			log.info(">>>>>>>>>>>>>uuidMap:"+uuidMap);
			long  total = 0;
			for (Entry<String, Integer> entry : uuidMap.entrySet()) {
				total = total + entry.getValue();
			}
//			log.info(">>>>>>>>>>>>>total:"+total);
			
			Map<String,Map<String,String>> csvMap = new LinkedHashMap<String,Map<String,String>>();
			try {
				Configuration conf = context.getConfiguration();
				FileSystem fs = FileSystem.get(conf);
				org.apache.hadoop.fs.Path category24MappingFile = new org.apache.hadoop.fs.Path("/home/webuser/dmp/jobfile/24h_menu-1.csv");
				FSDataInputStream fsDataInputStream = fs.open(category24MappingFile);
				InputStreamReader inr = new InputStreamReader(fsDataInputStream);
	            BufferedReader reader = new BufferedReader(inr);
	            String line = null;
	            
	    		while ((line = reader.readLine()) != null) {
	    			String item[] = line.split(",");
	    			String data0 = item[0].trim();
	    			String data1 = item[1].trim();
	    			String data2 = item[2].trim();
	    			String data3 = item[3].trim();
	    			String data4 = item[4].trim();
	    			String data5 = item[5].trim();
//	    			System.out.print(data0 + "," + data1 + "," + data2 + "," + data3 + "," + data4 + "," + data5 +"\n" );
	    			Map<String,String> detail = new LinkedHashMap<String,String>();
	    			detail.put("name_1", data0);
	    			detail.put("code_1", data1);
	    			detail.put("name_2", data2);
	    			detail.put("code_2", data3);
	    			detail.put("name_3", data4);
	    			detail.put("code_3", data5);
	    			csvMap.put(data5, detail);
	    		}
	    		reader.close();
			}catch(Exception e) {
				System.out.println("FAIL PROCESS MAP");
				System.out.println(e.getMessage());
			}
			
			try {
				Set<Entry<String, Map<String, String>>> csvMapSet = csvMap.entrySet();
	    		Iterator<Entry<String, Map<String, String>>> csvMapIterator = csvMapSet.iterator();
	    		while(csvMapIterator.hasNext()){
	    			Entry<String, Map<String, String>> entry = csvMapIterator.next();
	    			Map<String,String> detail = entry.getValue();
//	    			System.out.println(">>>>>>>>>>>>>detail:"+detail);
	    			
	    			Set<Entry<String, String>> detailMapSet = detail.entrySet();
	        		Iterator<Entry<String, String>> detailIterator = detailMapSet.iterator();
	        		
	        		Map<String,String> detailNew = new LinkedHashMap<String,String>();
	        		while(detailIterator.hasNext()){
	        			Entry<String, String> detailEntry = detailIterator.next();
						String key = detailEntry.getKey();
						String value = detailEntry.getValue();
						detailNew.put(key, value);
//						System.out.println(">>>>>>>>>>>>>key:"+key);
//						System.out.println(">>>>>>>>>>>>>value:"+value);
						if(uuidMap.containsKey(value)) {
							detailNew.put(key+"_UNI", String.valueOf(uuidMap.get(value)));
							detailNew.put(key+"_PV", String.valueOf(uuidMap.get(value+"_PV")));
						}else {
							detailNew.put(key+"_UNI", "0");
							detailNew.put(key+"_PV", "0");
						}
	        		}
	        		csvMap.put(entry.getKey(), detailNew);
	    		}
			}catch(Exception e) {
				System.out.println("FAIL ADD　PROCESS MAP");
				System.out.println(e.getMessage());
			}
    		
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>csvMap:"+csvMap.size());
    		
			int count = 0;
			for (Entry<String, Map<String, String>> entryMap : csvMap.entrySet()) {
				wiriteToDruid.setLength(0);
				for (Entry<String, String> entry : entryMap.getValue().entrySet()) {
					if(count == 0) {
						if(entry.getKey().equals("name_1")) {
							wiriteToDruid.append("\""+"館別名稱(第一層)"+"\"");
						}else if(entry.getKey().equals("code_1")) {
							wiriteToDruid.append(",").append("\"").append("館別代碼(第一層)").append("\"");
						}else if(entry.getKey().equals("code_1_UNI")) {
							wiriteToDruid.append(",").append("\"").append("不重複UUID(第一層)").append("\"");
						}else if(entry.getKey().equals("code_1_PV")) {
							wiriteToDruid.append(",").append("\"").append("曝光數(第一層)").append("\"");
						}else if(entry.getKey().equals("name_2")) {
							wiriteToDruid.append(",").append("\""+"館別名稱(第二層)"+"\"");
						}else if(entry.getKey().equals("code_2")) {
							wiriteToDruid.append(",").append("\"").append("館別代碼(第二層)").append("\"");
						}else if(entry.getKey().equals("code_2_UNI")) {
							wiriteToDruid.append(",").append("\"").append("不重複UUID(第二層)").append("\"");
						}else if(entry.getKey().equals("code_2_PV")) {
							wiriteToDruid.append(",").append("\"").append("曝光數(第二層)").append("\"");
						}else if(entry.getKey().equals("name_3")) {
							wiriteToDruid.append(",").append("\""+"館別名稱(第三層)"+"\"");
						}else if(entry.getKey().equals("code_3")) {
							wiriteToDruid.append(",").append("\"").append("館別代碼(第三層)").append("\"");
						}else if(entry.getKey().equals("code_3_UNI")) {
							wiriteToDruid.append(",").append("\"").append("不重複UUID(第三層)").append("\"");
						}else if(entry.getKey().equals("code_3_PV")) {
							wiriteToDruid.append(",").append("\"").append("曝光數(第三層)").append("\"");
						}
					}else if(count > 0){
						String entryValue = entry.getValue();
						if(entry.getKey().equals("name_1")) {
							wiriteToDruid.append("\""+"ALEX"+"\"");
						}else if(entry.getKey().equals("code_1")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("code_1_UNI")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("code_1_PV")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("name_2")) {
							wiriteToDruid.append(",").append("\""+"ALEX"+"\"");
						}else if(entry.getKey().equals("code_2")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("code_2_UNI")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("code_2_PV")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("name_3")) {
							wiriteToDruid.append(",").append("\""+"ALEX"+"\"");
						}else if(entry.getKey().equals("code_3")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("code_3_UNI")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}else if(entry.getKey().equals("code_3_PV")) {
							wiriteToDruid.append(",").append("\"").append(entryValue).append("\"");
						}
						
					}
				}
				if(wiriteToDruid.length() > 0) {
					context.write(new Text(wiriteToDruid.toString()), null);
					wiriteToDruid.setLength(0);
				}
				
				
				count = count + 1;
			}
    		
			
			
			
			
		} catch (Exception e) {
			log.error("reduce cleanup error>>>>>> " +e);
		}
	}
	
	
	
	
	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
}
