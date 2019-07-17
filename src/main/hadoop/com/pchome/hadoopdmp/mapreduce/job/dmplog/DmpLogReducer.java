package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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

import net.minidev.json.JSONObject;
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

	public int count;

	public JSONParser jsonParser = null;

	public String redisFountKey;


	public Map<String, Integer> redisClassifyMap = null;
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String[] weeks = {"SUN","MON","TUE","WED","THU","FRI","SAT"};
	private static Calendar calendar = Calendar.getInstance();
	private static StringBuffer wiriteToDruid = new StringBuffer();
	private static net.minidev.json.JSONObject dmpJSon =  new net.minidev.json.JSONObject();
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	private static DBCollection dBCollection_user_detail;
	private DB mongoOrgOperations;
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();
	public static Map<String, String> pfbxWebsiteCategory = new HashMap<String, String>();
	public static List<String> categoryLevelMappingList = new ArrayList<String>();
	public static Map<String, String> categoryLevelMappingMap = new HashMap<String, String>();
	
	private static int testCount = 0;
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
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
//			log.info(">>>>>>>>>>>dmpJSon:"+dmpJSon);
//			log.info(">>>>>>>>>>>mapperKey:"+mapperKey.toString());
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
					personalInfoComponent.processPersonalInfo(dmpJSon, dBCollection_user_detail);
				}catch(Exception e) {
					log.error(">>>>>>>fail process processPersonalInfo:"+e.getMessage());
					continue;
				}
				
				calendar.setTime(sdf.parse(dmpJSon.getAsString("log_date")));
				int week_index = calendar.get(Calendar.DAY_OF_WEEK) - 1;
				if(week_index<0){
					week_index = 0;
				}
				String pfbxCustomerInfoId = dmpJSon.getAsString("pfbx_customer_info_id");
				String webClass = StringUtils.isBlank(pfbxWebsiteCategory.get(pfbxCustomerInfoId)) ? "" : pfbxWebsiteCategory.get(pfbxCustomerInfoId);
				
				
				//log.info(dmpJSon);
				wiriteToDruid.append("\""+dmpJSon.getAsString("uuid").toString()+"\"".trim());
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("memid")).append("\"");
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
				wiriteToDruid.append(",").append("\"").append(weeks[week_index]).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_ck")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_pv")).append("\"");
				wiriteToDruid.append(",").append("\"").append(webClass).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("mark_layer1")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("mark_layer2")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("mark_layer3")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("mark_layer4")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_id")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_price")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_dis")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("classify")).append("\"");
				wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("behavior")).append("\"");
				//產出csv
				keyOut.set("\""+dmpJSon.getAsString("uuid")+"\"".trim());
				context.write(new Text(wiriteToDruid.toString()), null);
				
				
				if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_layer3")) && testCount < 1) {
					log.info(">>>>>>>>>>>>>>> alex:"+dmpJSon);
					testCount += 1;
				}
				
			}
			dmpJSon.clear();
			wiriteToDruid.setLength(0);
		} catch (Throwable e) {
			 log.error(">>>>>> reduce error :"+e.getMessage());
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
