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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import com.pchome.hadoopdmp.mapreduce.job.pacllog.PcalConditionBean;
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
	public RedisTemplate<String, Object> redisTemplate = null;
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
	public static int bu_log_count = 0;
	public static int kdcl_log_count = 0;
	public static int pack_log_count = 0;
	
	
	public static MysqlUtil mysqlUtil = null;
	
	public static Map<String, PcalConditionBean> convertConditionMap = new HashMap<String, PcalConditionBean>();
	private StringBuffer userDefineConvertDbInfoSqlStr = new StringBuffer();
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			dBCollection_user_detail = this.mongoOrgOperations.getCollection("user_detail");
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
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(context.getConfiguration().get("spring.profiles.active"));
			StringBuffer findWebClassCodeSqlStr = new StringBuffer();
			findWebClassCodeSqlStr.append(" SELECT a.customer_info_id,a.category_code FROM pfbx_allow_url a WHERE 1 = 1 and a.default_type = 'Y' ORDER BY a.customer_info_id  ");
			ResultSet resultSet = mysqlUtil.query(findWebClassCodeSqlStr.toString());
			while(resultSet.next()){
				pfbxWebsiteCategory.put(resultSet.getString("customer_info_id"), resultSet.getString("category_code"));
			}
			
			//查詢轉換相關資訊
			userDefineConvertDbInfoSqlStr.append(" SELECT   ");
			userDefineConvertDbInfoSqlStr.append(" 	c.convert_type,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.convert_seq,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.click_range_date,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.imp_range_date,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.convert_price,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.convert_status,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.convert_belong,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.convert_num_type,  ");
			userDefineConvertDbInfoSqlStr.append(" 	Group_concat(convert_rule_id SEPARATOR ':')convert_rule_id,  ");
			userDefineConvertDbInfoSqlStr.append(" 	c.pfp_customer_info_id  ");
			userDefineConvertDbInfoSqlStr.append(" FROM   pfp_code_convert c  ");
			userDefineConvertDbInfoSqlStr.append(" LEFT JOIN pfp_code_convert_rule r  ");
			userDefineConvertDbInfoSqlStr.append(" ON( c.convert_seq = r.convert_seq )  ");
			userDefineConvertDbInfoSqlStr.append(" WHERE  1 = 1  ");
			userDefineConvertDbInfoSqlStr.append(" AND c.convert_seq = 'CONVERT_ID'");
			userDefineConvertDbInfoSqlStr.append(" GROUP  BY r.convert_seq  ");
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}
	
	private static int count = 0;
	private static Set<String> uuidSet = new HashSet<String>();
	private static long uuidPv = 0;
	
	private static Map<String,Integer> uuidMap = new HashMap<String,Integer>();
	
	
	private static List<net.minidev.json.JSONObject> logJson = new ArrayList<net.minidev.json.JSONObject>();
	@Override
	public void reduce(Text uuidKey, Iterable<Text> dmpJsonStr, Context context) {
		try {
			
			
			for (Text text : dmpJsonStr) {
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
				dmpJSon.put("web_class", webClass);
				dmpJSon.put("week_day", weeks[week_index]);
				
				
				
				if(dmpJSon.getAsString("event_id").equals("CAC20181210000000001")) {
					System.out.println(dmpJSon);
				}
				
				
				
			}
			
				
				
				
				
				
				
				
				
				
				
				
//				//產出csv
//				if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_value"))) { //BULOG
//					for (int i= 0; i < markLevelList.length; i++) {
//						if(StringUtils.isNotBlank(dmpJSon.getAsString(markLevelList[i]))) {
//							if(dmpJSon.getAsString("mark_value").equals(dmpJSon.getAsString(markValueList[i]))) {
//								dmpJSon.put("pv", 1);
//							}else {
//								dmpJSon.put("pv", 0);
//							}
//							wiriteToDruid.append("\""+dmpJSon.getAsString("fileName")+"\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.get("memid")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid_flag")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ip")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("url")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("referer")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("domain")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_source")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("trigger_type")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfp_customer_info_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfd_customer_info_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_customer_info_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("style_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("action_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("group_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_position_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_country")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_city")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_phone_info")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_os_info")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_browser_info")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex_source")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age_source")).append("\"");
//							wiriteToDruid.append(",").append("\"").append("audicen_id_default").append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_x")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_y")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_event")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("event_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_id")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_price")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_dis")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString(markValueList[i])).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString(markLevelList[i])).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op1")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op2")).append("\"");
//							wiriteToDruid.append(",").append("\"").append("ad_price_default").append("\"");
//							wiriteToDruid.append(",").append("\"").append(webClass).append("\"");
//							wiriteToDruid.append(",").append("\"").append(weeks[week_index]).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ck")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pv")).append("\"");
//							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("category")).append("\"");
//							context.write(new Text(wiriteToDruid.toString()), null);
//							wiriteToDruid.setLength(0);
//							
//							if(dmpJSon.getAsString("log_source").equals("pacl_log")) {
//								pack_log_count = pack_log_count + 1;
//							}else if(dmpJSon.getAsString("log_source").equals("kdcl_log")) {
//								kdcl_log_count = kdcl_log_count + 1;
//							}else if(dmpJSon.getAsString("log_source").equals("bu_log")) {
//								bu_log_count = bu_log_count + 1;
//							}
//						}
//					}
//				}else {
//					wiriteToDruid.append("\""+dmpJSon.getAsString("fileName")+"\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
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
//					wiriteToDruid.append(",").append("\"").append(webClass).append("\"");
//					wiriteToDruid.append(",").append("\"").append(weeks[week_index]).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ck")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pv")).append("\"");
//					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("category")).append("\"");
//					context.write(new Text(wiriteToDruid.toString()), null);
//					wiriteToDruid.setLength(0);
//					
//					if(dmpJSon.getAsString("log_source").equals("pacl_log")) {
//						pack_log_count = pack_log_count + 1;
//					}else if(dmpJSon.getAsString("log_source").equals("kdcl_log")) {
//						kdcl_log_count = kdcl_log_count + 1;
//					}else if(dmpJSon.getAsString("log_source").equals("bu_log")) {
//						bu_log_count = bu_log_count + 1;
//					}
//				}
			
		} catch (Throwable e) {
			 log.error(">>>>>> reduce error :"+e.getMessage());
		}
	}
	
	
	public void cleanup(Context context) {
		try {
			mysqlUtil.closeConnection();
			
			System.out.println("total bu_log  >>>>>>>>>>>>>>>>>>>>>>>>"+bu_log_count);
			System.out.println("total kdcl_log>>>>>>>>>>>>>>>>>>>>>>>>"+kdcl_log_count);
			System.out.println("total pack_log>>>>>>>>>>>>>>>>>>>>>>>>"+pack_log_count);
			
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
