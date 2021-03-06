package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
	public static String record_date;
	public static Producer<String, String> producer = null;
	public RedisTemplate<String, Object> redisTemplate = null;
	public JSONParser jsonParser = null;
	public String redisFountKey;
	private static String[] weeks = {"SUN","MON","TUE","WED","THU","FRI","SAT"};
	private static String[] markLevelList = {"mark_layer1","mark_layer2","mark_layer3","mark_layer4"};
	private static String[] markValueList = {"mark_value1","mark_value2","mark_value3","mark_value4"};
	private static Calendar calendar = Calendar.getInstance();
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static StringBuffer wiriteToDruid = new StringBuffer();
	public static StringBuffer industrySqlBuffer = new StringBuffer();
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();
	public static Map<String, String> pfbxWebsiteCategory = new HashMap<String, String>();
	public Map<String, Integer> redisClassifyMap = null;
	private static net.minidev.json.JSONObject dmpJSon =  new net.minidev.json.JSONObject();
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	private static DBCollection dBCollection_user_detail;
	private DB mongoOrgOperations;
	private static int bu_log_count = 0;
	private static int kdcl_log_count = 0;
	private static int pack_log_count = 0;
	private static Map<String,String> industryMap = new HashMap<String, String>();
	private MysqlUtil mysqlUtil = null;
	
	public static String record_hour;
	public static Map<String,String> markValueMap = new HashMap<String, String>();
	private static long pvclk_pv_count = 0;
	private static long pvclk_ck_count = 0;
	private static long pvclk_total_count = 0;
	
	private static long debug_total_count = 0;
	
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			record_date = context.getConfiguration().get("job.date");
			record_hour = context.getConfiguration().get("job.hour");
			System.out.println("record_date:" + record_date);
			System.out.println("record_hour:" + record_hour);
			
			
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
			StringBuffer sql = new StringBuffer();
			sql.append(" SELECT a.customer_info_id,a.category_code FROM pfbx_allow_url a WHERE 1 = 1 and a.default_type = 'Y' and a.category_code  !='' ORDER BY a.customer_info_id  ");
			ResultSet resultSet = mysqlUtil.query(sql.toString());
			while(resultSet.next()){
				pfbxWebsiteCategory.put(resultSet.getString("customer_info_id"), resultSet.getString("category_code"));
			}
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}
	
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
				
				if(dmpJSon.getAsString("log_source").equals("kdcl_log")) {
					if(dmpJSon.getAsString("trigger_type").equals("pv")) {
						pvclk_pv_count = pvclk_pv_count + Integer.parseInt(dmpJSon.getAsString("pv"));
					}else if(dmpJSon.getAsString("trigger_type").equals("ck")){
						pvclk_ck_count = pvclk_ck_count + Integer.parseInt(dmpJSon.getAsString("ck"));
					}
					pvclk_total_count = pvclk_total_count + 1;
				}
				
				//6.個資
				try {
					personalInfoComponent.processPersonalInfo(dmpJSon, dBCollection_user_detail);
				}catch(Exception e) {
					log.error(">>>>>>>fail process processPersonalInfo:"+e.getMessage());
					continue;
				}
				//7.產業別
				try {
					if(industryMap.containsKey(dmpJSon.getAsString("pfp_customer_info_id"))) {
						dmpJSon.put("industry",industryMap.get(dmpJSon.getAsString("pfp_customer_info_id")));
					}else {
						industrySqlBuffer.setLength(0);
						industrySqlBuffer.append(" SELECT industry FROM pfp_customer_info where 1 =1 and customer_info_id = '").append(dmpJSon.getAsString("pfp_customer_info_id")).append("'");
						ResultSet resultSet = mysqlUtil.query(industrySqlBuffer.toString());
						while(resultSet.next()){
							String industry = resultSet.getString("industry");
							for (EnumAccountIndustry enumAccountIndustry : EnumAccountIndustry.values()) {
								if(industry.contentEquals(enumAccountIndustry.getName())) {
									industryMap.put(dmpJSon.getAsString("pfp_customer_info_id"), enumAccountIndustry.getValue());
									dmpJSon.put("industry",enumAccountIndustry.getValue());
									break;
								}
							}
						}
					}
				}catch(Exception e) {
					log.error(">>>>>>>fail process industry:"+e.getMessage());
					continue;
				}
				
				calendar.setTime(sdf.parse(dmpJSon.getAsString("log_date")));
				int week_index = calendar.get(Calendar.DAY_OF_WEEK) - 1;
				if(week_index<0){
					week_index = 0;
				}
				String pfbxCustomerInfoId = dmpJSon.getAsString("pfbx_customer_info_id");
				String webClass = StringUtils.isBlank(pfbxWebsiteCategory.get(pfbxCustomerInfoId)) ? "" : pfbxWebsiteCategory.get(pfbxCustomerInfoId);
				
				//產出bu log csv 
				if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_value")) && dmpJSon.getAsString("log_source").equals("bu_log")) {
					if(dmpJSon.getAsString("mark_value").indexOf("-") >= 0) {
						System.out.println("debug mark_value:"+dmpJSon.getAsString("mark_value"));
					}
					for (int i= 0; i < markLevelList.length; i++) {
						wiriteToDruid.setLength(0);
						if(StringUtils.isNotBlank(dmpJSon.getAsString(markLevelList[i]))) {
							markValueMap.put(dmpJSon.getAsString("mark_value"), dmpJSon.getAsString("mark_value"));
							//fileName
							wiriteToDruid.append("\""+dmpJSon.getAsString("fileName")+"\"");
							//log date source
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
							//memid
							wiriteToDruid.append(",").append("\"").append(dmpJSon.get("memid")).append("\"");
							//uuid
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid")).append("\"");
							//uuid times flag
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid_flag")).append("\"");
							//ip
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ip")).append("\"");
							//url
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("url")).append("\"");
							//referer
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("referer")).append("\"");
							//domain
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("domain")).append("\"");
							//log type
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_source")).append("\"");
							//trogger type
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("trigger_type")).append("\"");
							//pfp customer_info_id
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfp_customer_info_id")).append("\"");
							////pfd customer_info_id
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfd_customer_info_id")).append("\"");
							//pfb customer_info_id
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_customer_info_id")).append("\"");
							//style_id
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("style_id")).append("\"");
							//action seq
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("action_id")).append("\"");
							//group seq
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("group_id")).append("\"");
							//ad seq
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_id")).append("\"");
							//position seq
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_position_id")).append("\"");
							//country
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_country")).append("\"");
							//city
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_city")).append("\"");
							//device
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info")).append("\"");
							//phone os
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_phone_info")).append("\"");
							//os
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_os_info")).append("\"");
							//browser
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_browser_info")).append("\"");
							//sex
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex")).append("\"");
							//sex source
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex_source")).append("\"");
							//age
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age")).append("\"");
							//age source
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age_source")).append("\"");
							//audicen
							wiriteToDruid.append(",").append("\"").append("").append("\"");
							//paid
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_id")).append("\"");
							//screen_x
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_x")).append("\"");
							//screen_y
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_y")).append("\"");
							//pa event
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_event")).append("\"");
							//event id
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("event_id")).append("\"");
							//prod id
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_id")).append("\"");
							//prod price
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_price")).append("\"");
							//prod dis
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_dis")).append("\"");
							//markValue
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString(markValueList[i])).append("\"");
							//marklevel
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString(markLevelList[i])).append("\"");
							//op1
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op1")).append("\"");
							//op2
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op2")).append("\"");
							//ad price
							wiriteToDruid.append(",").append("\"").append("").append("\"");
							//webClass
							wiriteToDruid.append(",").append("\"").append(webClass).append("\"");
							//week
							wiriteToDruid.append(",").append("\"").append(weeks[week_index]).append("\"");
							//ad_view
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
							//vpv
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
							//ck
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ck")).append("\"");
							//pv
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pv")).append("\"");
							//category
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("category")).append("\"");
							//convert_price
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("convert_price")).append("\"");
							//device_type
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_type")).append("\"");
//							//判斷目前為第幾層資料
							//brand
							if(i == 0 && StringUtils.isNotBlank(dmpJSon.getAsString("level_1_brand"))) {
								wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("level_1_brand")).append("\"");
							}else if(i == 1 && StringUtils.isNotBlank(dmpJSon.getAsString("level_2_brand"))) {
								wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("level_2_brand")).append("\"");
							}else if(i == 2 && StringUtils.isNotBlank(dmpJSon.getAsString("level_3_brand"))) {
								wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("level_3_brand")).append("\"");
							}else if(i == 3 && StringUtils.isNotBlank(dmpJSon.getAsString("level_4_brand"))) {
								wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("level_4_brand")).append("\"");
							}else {
								wiriteToDruid.append(",").append("\"").append("").append("\"");
							}
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("industry")).append("\"");
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("24h_price_code")).append("\"");
							
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("hour")).append("\"");
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("source_date")).append("\"");
							wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("rule_id")).append("\"");
							context.write(new Text(wiriteToDruid.toString()), null);
							wiriteToDruid.setLength(0);
							
							if(dmpJSon.getAsString("log_source").equals("pacl_log")) {
								pack_log_count = pack_log_count + 1;
							}else if(dmpJSon.getAsString("log_source").equals("kdcl_log")) {
								kdcl_log_count = kdcl_log_count + 1;
							}else if(dmpJSon.getAsString("log_source").equals("bu_log")) {
								bu_log_count = bu_log_count + 1;
							}
						}
					}
				}else {
					wiriteToDruid.append("\""+dmpJSon.getAsString("fileName")+"\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_date")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.get("memid")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("uuid_flag")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ip")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("url")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("referer")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("domain")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("log_source")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("trigger_type")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfp_customer_info_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfd_customer_info_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_customer_info_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("style_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("action_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("group_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pfbx_position_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_country")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("area_city")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_info")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_phone_info")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_os_info")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_browser_info")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("sex_source")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("age_source")).append("\"");
					wiriteToDruid.append(",").append("\"").append("").append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_x")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("screen_y")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pa_event")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("event_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_id")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_price")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("prod_dis")).append("\"");
					if(StringUtils.isNotBlank(dmpJSon.getAsString("mark_value"))) {
						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("mark_value")).append("\"");
					}else {
						wiriteToDruid.append(",").append("\"").append("").append("\"");	
					}
					if(StringUtils.isNotBlank(dmpJSon.getAsString("pacl_mark_layer"))) {
						wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pacl_mark_layer")).append("\"");
					}else {
						wiriteToDruid.append(",").append("\"").append("").append("\"");	
					}
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op1")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("op2")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_price")).append("\"");
					wiriteToDruid.append(",").append("\"").append(webClass).append("\"");
					wiriteToDruid.append(",").append("\"").append(weeks[week_index]).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ad_view")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("vpv")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("ck")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("pv")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("category")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("convert_price")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("device_type")).append("\"");
					//非bulog沒有mark_value，沒有品牌可對應
					wiriteToDruid.append(",").append("\"").append("").append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("industry")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("24h_price_code")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("hour")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("source_date")).append("\"");
					wiriteToDruid.append(",").append("\"").append(dmpJSon.getAsString("rule_id")).append("\"");
					
					context.write(new Text(wiriteToDruid.toString()), null);
					wiriteToDruid.setLength(0);
					
					if(dmpJSon.getAsString("log_source").equals("pacl_log")) {
						pack_log_count = pack_log_count + 1;
					}else if(dmpJSon.getAsString("log_source").equals("kdcl_log")) {
						kdcl_log_count = kdcl_log_count + 1;
					}else if(dmpJSon.getAsString("log_source").equals("bu_log")) {
						bu_log_count = bu_log_count + 1;
					}
				}
			}
		} catch (Throwable e) {
			 log.error(">>>>>> reduce error :"+e.getMessage());
		}
	}
	
	public void cleanup(Context context) {
		try {
			this.mysqlUtil.closeConnection();
			System.out.println("pvclk_pv_count >>>>>>>>>>>>>>>>>>>>>>>"+pvclk_pv_count);
			System.out.println("pvclk_ck_count >>>>>>>>>>>>>>>>>>>>>>>"+pvclk_ck_count);
			
			System.out.println("total bu_log  >>>>>>>>>>>>>>>>>>>>>>>>"+bu_log_count);
			System.out.println("total kdcl_log>>>>>>>>>>>>>>>>>>>>>>>>"+kdcl_log_count);
			System.out.println("total pack_log>>>>>>>>>>>>>>>>>>>>>>>>"+pack_log_count);
			System.out.println("markValueMap >>>>>>>>>>>>>>>>>>>>>>>>"+markValueMap);
		} catch (Exception e) {
			log.error("reduce cleanup error>>>>>> " +e);
		}
	}
	
	//年齡推估
	public class combinedValue {
		public String gender;
		public String age;
		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
}
