package com.pchome.hadoopdmp.mapreduce.job.dmplog;


import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;
import java.io.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.pchome.hadoopdmp.enumerate.CategoryLogEnum;
import com.pchome.hadoopdmp.mapreduce.job.component.DeviceComponent;
import com.pchome.hadoopdmp.mapreduce.job.component.GeoIpComponent;
import com.pchome.hadoopdmp.mapreduce.job.factory.ACategoryLogData;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryCodeBean;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogFactory;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryRutenCodeBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodborg.MongodbOrgHadoopConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import net.minidev.json.JSONObject;
import org.apache.log4j.Logger;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.conf.Configuration;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.File;
import java.io.InputStreamReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
@Component
public class DmpLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	
//	private static int recordCount = 0;
//	private static int kdclLogLength = 30;
//	private static int campaignLogLength = 9;
	

//	private Text keyOut = new Text();
//	private Text valueOut = new Text();
//
//	public static String record_date;
//	public static String record_hour;
//	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
//	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
//	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
//	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();	 //Ruten分類表
//	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
//	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
//	public static DateTimeComponent dateTimeComponent = new DateTimeComponent();
//	public static DeviceComponent deviceComponent = new DeviceComponent();
//	private DB mongoOrgOperations;
//	public static DatabaseReader reader = null;
//	public static InetAddress ipAddress = null;
//	private static DBCollection dBCollection_class_url;
//	private static InputSplit inputSplit;
//	public static List<String> categoryLevelMappingList = new ArrayList<String>();
//	public static Map<String, JSONObject> categoryLevelMappingMap = new HashMap<String, JSONObject>();
//	public static ACategoryLogData aCategoryLogDataClick = null;
//	public static ACategoryLogData aCategoryLogDataRetun = null;
//	public static ACategoryLogData aCategoryLogData24H = null;
//	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	
	
//	private static int totalCount= 0;
//	private static String[] markLevelList = {"mark_layer1","mark_layer2","mark_layer3"};
//	private static String[] markValueList = {"mark_value1","mark_value2","mark_value3"};
	
	private static Logger log = Logger.getLogger(DmpLogMapper.class);
	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
	public static DeviceComponent deviceComponent = new DeviceComponent();
	public static String record_date;
	public static String record_hour;
	public static DatabaseReader reader = null;
	public static InetAddress ipAddress = null;
	private static String logpath = "";
	private static String logStr = "";
	private static String[] values  = null;
	private static InputSplit inputSplit;
	private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	private static int kdclLogLength = 30;
	private static int campaignLogLength = 9;
	private static Map<String,String> hostNameMap = new HashMap<String,String>();
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();
	public static List<String> categoryLevelMappingList = new ArrayList<String>();
	public static ACategoryLogData aCategoryLogDataClick = null;
	public static ACategoryLogData aCategoryLogDataRetun = null;
	public static ACategoryLogData aCategoryLogData24H = null;
	private DB mongoOrgOperations;
	private static DBCollection dBCollection_class_url;
	public static Map<String, JSONObject> categoryLevelMappingMap = new HashMap<String, JSONObject>();
	
	
	private static net.minidev.json.JSONObject json =  new net.minidev.json.JSONObject();
	private static net.minidev.json.JSONObject dmpDataJson999 =  new net.minidev.json.JSONObject();
	private static net.minidev.json.JSONObject dmpDataJson_998 = new net.minidev.json.JSONObject();
	
	
	
	public void setup(Context context) {
		System.out.println(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.out.println("*********>>>>>>>>>>>:"+System.currentTimeMillis());
			record_date = context.getConfiguration().get("job.date");
			record_hour = context.getConfiguration().get("job.hour");
			System.out.println("record_date:" + record_date);
			System.out.println("record_hour:"+record_hour);
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.aCategoryLogDataClick = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
			this.aCategoryLogDataRetun = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
			this.aCategoryLogData24H = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
//			
			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			dBCollection_class_url =  this.mongoOrgOperations.getCollection("class_url");
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
			// load 分類表(pfp_ad_category_new.csv)
			Path cate_path = Paths.get(path[0].toString());
			charset = Charset.forName("UTF-8");
			int maxCateLvl = 4;
			categoryList = new ArrayList<Map<String, String>>();
			for (int i = 0; i < maxCateLvl; i++) {
				categoryList.add(new HashMap<String, String>());
			}
			lines.clear();
			lines = Files.readAllLines(cate_path, charset);
			
			// 將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
			for (String line : lines) {
				String[] tmpStr = line.split(";");
				int lvl = Integer.parseInt(tmpStr[5].replaceAll("\"", "").trim());
				if (lvl <= maxCateLvl) {
					categoryList.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
				}
			}
			
			// load 24h分類表(DMP_24h_category.csv)
			Path category24HPath = Paths.get(path[3].toString());
			List<String> lines24H = Files.readAllLines(category24HPath, charset);
			for (String line : lines24H) {
				CategoryCodeBean categoryBean = new CategoryCodeBean();
				String[] tmpStrAry = line.split(","); // 0001000000000000;M,35
				categoryBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
				categoryBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
				categoryBean.setBreadCrumb(tmpStrAry[2].replaceAll("\"", ""));
				categoryBean.setEnglishCode(tmpStrAry[3].replaceAll("\"", ""));
				category24hBeanList.add(categoryBean);
			}
			// load Ruten分類表(DMP_Ruten_category.csv)
			Path categoryRutenPath = Paths.get(path[4].toString());
			List<String> linesRuten = Files.readAllLines(categoryRutenPath, charset);
			for (String line : linesRuten) {
				CategoryRutenCodeBean categoryRutenBean = new CategoryRutenCodeBean();
				String[] tmpStrAry = line.split(","); //"0001000000000000","電腦、電腦周邊"
				categoryRutenBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
				categoryRutenBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
				categoryRutenBeanList.add(categoryRutenBean);
			}
			//IP轉城市
			File database = new File(path[5].toString());
			reader = new DatabaseReader.Builder(database).build();  
			//24館別階層對應表
			FileSystem fs = FileSystem.get(conf);
			org.apache.hadoop.fs.Path category24MappingFile = new org.apache.hadoop.fs.Path("hdfs://druid1.mypchome.com.tw:9000/hadoop_file/24h_menu-1.csv");
			FSDataInputStream inputStream = fs.open(category24MappingFile);
			Reader reader = new InputStreamReader(inputStream);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
			for (CSVRecord csvRecord : csvParser) {
				String data = csvRecord.get(1)+"<PCHOME>"+csvRecord.get(3)+"<PCHOME>"+csvRecord.get(5);
				categoryLevelMappingList.add(data);
			}
		} catch (Exception e) {
			System.out.println("Mapper setup error>>>>>> " + e.getMessage());
		}
	}
	
	
	public synchronized void map(LongWritable offset, Text value, Context context) {
//		清空mapper中json資料
//		System.out.println("---------***---------");
//		json.put("alex", "5555");
//		System.out.println(json.get("alex"));
//		
//		dmpDataJson999.put("alex", "8888");
//		System.out.println(dmpDataJson999.get("alex"));
//		
//		
//		dmpDataJson_998.put("alex", "777");
//		System.out.println(dmpDataJson_998.get("alex"));
//		
////		dmpDataJson.clear();
//		inputSplit = (InputSplit)context.getInputSplit(); 
//		logpath = ((FileSplit)inputSplit).getPath().toString();
//		String fileName = ((FileSplit)inputSplit).getPath().getName();
//		System.out.println(fileName);
		
		
////		String fileName = ((FileSplit)inputSplit).getPath().getName();
////		logStr = "";
////		logStr = value.toString();
////		
//		dmpDataJson.put("fileName", "alex");
//		System.out.println(dmpDataJson.getAsString("fileName"));
		
		
//			dmpDataJson_998.clear();
			inputSplit = (InputSplit)context.getInputSplit(); 
			logpath = ((FileSplit)inputSplit).getPath().toString();
			String fileName = ((FileSplit)inputSplit).getPath().getName();
			values = null;
			logStr = "";
			logStr = value.toString();
			
			dmpDataJson_998.put("fileName", "");
			dmpDataJson_998.put("log_date", "");
			dmpDataJson_998.put("hour", "");
			dmpDataJson_998.put("memid", "");
			dmpDataJson_998.put("uuid", "");
			dmpDataJson_998.put("uuid_flag", "");
			dmpDataJson_998.put("url", "");
			dmpDataJson_998.put("referer", "");
			dmpDataJson_998.put("domain", "");
			dmpDataJson_998.put("log_source", "");
			dmpDataJson_998.put("pfd_customer_info_id", "");
			dmpDataJson_998.put("pfp_customer_info_id", "");
			dmpDataJson_998.put("style_id", "");
			dmpDataJson_998.put("action_id", "");
			dmpDataJson_998.put("group_id", "");
			dmpDataJson_998.put("ad_id", "");
			dmpDataJson_998.put("pfbx_customer_info_id", "");
			dmpDataJson_998.put("pfbx_position_id", "");
			dmpDataJson_998.put("ad_view", "");
			dmpDataJson_998.put("vpv", "");
			dmpDataJson_998.put("screen_x", "");
			dmpDataJson_998.put("screen_y", "");
			dmpDataJson_998.put("event_id", "");
			dmpDataJson_998.put("trigger_type", "");
			dmpDataJson_998.put("ck", 0);
			dmpDataJson_998.put("pv", 0);
			dmpDataJson_998.put("ad_class", "");
			dmpDataJson_998.put("ip", "");
			dmpDataJson_998.put("area_country", "");
			dmpDataJson_998.put("area_city", "");
			dmpDataJson_998.put("area_info_source", "");
			dmpDataJson_998.put("area_info_classify", "");
			dmpDataJson_998.put("user_agent", "");
			dmpDataJson_998.put("device_info", "");
			dmpDataJson_998.put("device_phone_info", "");
			dmpDataJson_998.put("device_os_info", "");
			dmpDataJson_998.put("device_browser_info", "");
			dmpDataJson_998.put("device_info_source", "");
			dmpDataJson_998.put("device_info_classify", "");
			dmpDataJson_998.put("category", "");
			dmpDataJson_998.put("class_adclick_classify", "");
			dmpDataJson_998.put("category_source", "");
			dmpDataJson_998.put("sex", "");
			dmpDataJson_998.put("sex_source", "");
			dmpDataJson_998.put("age", "");
			dmpDataJson_998.put("age_source", "");
			dmpDataJson_998.put("personal_info_api_classify", "");
			dmpDataJson_998.put("pa_id", "");
			dmpDataJson_998.put("screen_x", "");
			dmpDataJson_998.put("screen_y", "");
			dmpDataJson_998.put("pa_event", "");
			dmpDataJson_998.put("event_id", "");
			dmpDataJson_998.put("prod_id", "");
			dmpDataJson_998.put("prod_price", "");
			dmpDataJson_998.put("prod_dis", "");
			dmpDataJson_998.put("op1", "");
			dmpDataJson_998.put("op2", "");
			dmpDataJson_998.put("email", "");
			dmpDataJson_998.put("mark_value", "");
			dmpDataJson_998.put("mark_layer1", "");
			dmpDataJson_998.put("mark_layer2", "");
			dmpDataJson_998.put("mark_layer3", "");
			dmpDataJson_998.put("mark_layer4", "");
			
			if(logpath.contains("kdcl_log")) {
				try {
					//kdcl log	raw data格式為一般或是Campaign
					if(logStr.indexOf(kdclSymbol) > -1 ){
						// values[0]  date time (2018-01-04 04:57:12)
						// values[1]  memid
						// values[2]  uuid
						// values[3]  ip
						// values[4]  referer
						// values[5]  UserAgent
						// values[13] ck,pv
						// values[15] ad_class
						this.values = this.logStr.split(kdclSymbol,-1);
						if (values.length < kdclLogLength) {
							return;
						}
						if ((StringUtils.equals(values[1], "null")||StringUtils.isBlank(values[1]) ) && (StringUtils.equals(values[2], "null")||StringUtils.isBlank(values[2])) ){
							return;
						}
						if (StringUtils.isBlank(values[4]) || !(values[4].contains("http"))) {
							return;
						}
						
						dmpDataJson_998.put("fileName", fileName);
						dmpDataJson_998.put("log_date", values[0]);
						dmpDataJson_998.put("memid", values[1]);
						dmpDataJson_998.put("uuid", values[2]);
						if(values[2].contains("xxx-")) {
							dmpDataJson_998.put("uuid_flag", "y");
						}else {
							dmpDataJson_998.put("uuid_flag", "n");
						}
						dmpDataJson_998.put("referer", values[4]);
						try {
							if(hostNameMap.containsKey(values[4].toString())) {
								dmpDataJson_998.put("domain", hostNameMap.get(values[4].toString()));
							}else {
								URI uri = new URI(values[4]);
								String domain = uri.getHost();
								dmpDataJson_998.put("domain", domain.startsWith("www.") ? domain.substring(4) : domain);
								hostNameMap.put(values[4].toString(), domain.startsWith("www.") ? domain.substring(4) : domain);
							}
						}catch(Exception e) {
							System.out.println("kdcl log process domain fail:"+e.getMessage());
							System.out.println("kdcl log process domain fail json:"+dmpDataJson_998);
							return;
						}
						dmpDataJson_998.put("log_source", "kdcl_log");
						dmpDataJson_998.put("pfd_customer_info_id", values[24]);
						dmpDataJson_998.put("pfp_customer_info_id", values[6]);
						dmpDataJson_998.put("style_id", values[7]);
						dmpDataJson_998.put("action_id", values[21]);
						dmpDataJson_998.put("group_id", values[22]);
						dmpDataJson_998.put("ad_id", values[11]);
						dmpDataJson_998.put("pfbx_customer_info_id", values[25]);
						dmpDataJson_998.put("pfbx_position_id", values[26]);
						dmpDataJson_998.put("ad_view", values[45]);
						dmpDataJson_998.put("vpv", values[46]);
						dmpDataJson_998.put("trigger_type", values[13]);
						if(values[13].toUpperCase().equals("CK")) {
							dmpDataJson_998.put("ck", 1);
							dmpDataJson_998.put("pv", 0);
						}else if(values[13].toUpperCase().equals("PV")) {
							dmpDataJson_998.put("ck", 0);
							dmpDataJson_998.put("pv", 1);
						}
						dmpDataJson_998.put("ad_class", values[15]);
						dmpDataJson_998.put("ip", values[3]);
						dmpDataJson_998.put("area_info_source", "ip");
						//裝置資訊 [device_info_classify] null:user_agent為空
						dmpDataJson_998.put("user_agent", values[5].replaceAll("\"", ""));
						if(values[4].contains("24h.pchome.com.tw")) {
							String pageCategory = "";
							if(values[4].equals("https://24h.pchome.com.tw/") || values[4].contains("htm") || values[4].contains("index") || values[4].contains("?fq=") || values[4].contains("store/?q=")) {
								return;
							}else if(values[4].contains("?")) {
								pageCategory = values[4].split("/")[values[4].split("/").length - 1];
								pageCategory = pageCategory.substring(0, pageCategory.indexOf("?"));
							}else {
								pageCategory = values[4].split("/")[values[4].split("/").length - 1];
							}
							dmpDataJson_998.put("op1", pageCategory);
						}
					}else {
						return;
					}
				}catch(Exception e) {
					System.out.println(">>>> kdcl set json fail:"+dmpDataJson_998);
				}
			}else if(logpath.contains("pacl_log") ) {
//				try {
//					this.values = this.logStr.split(paclSymbol,-1);
//					dmpDataJson_998.put("fileName", fileName);
//					dmpDataJson_998.put("log_date", values[0]);
//					dmpDataJson_998.put("memid","");
//					dmpDataJson_998.put("uuid", values[2]);
//					if(values[2].contains("xxx-")) {
//						dmpDataJson_998.put("uuid_flag", "y");
//					}else {
//						dmpDataJson_998.put("uuid_flag", "n");
//					}
//					dmpDataJson_998.put("url", values[6]);
//					dmpDataJson_998.put("referer", values[5]);
//					dmpDataJson_998.put("domain", values[7]);
//					dmpDataJson_998.put("log_source", "pacl_log");
//					dmpDataJson_998.put("ad_view", 0);
//					dmpDataJson_998.put("vpv", 0);
//					dmpDataJson_998.put("trigger_type", "pv");
//					dmpDataJson_998.put("ck", 0);
//					dmpDataJson_998.put("pv", 1);
//					//地區資訊 [area_info_classify] null:ip不正確,N:ip比對不到
//					dmpDataJson_998.put("ip", values[1]);
//					dmpDataJson_998.put("area_info_source", "ip");
//					//裝置資訊 [device_info_classify] null:user_agent為空
//					dmpDataJson_998.put("user_agent", values[8].replaceAll("\"", ""));
//					dmpDataJson_998.put("pa_id", values[4]);
//					dmpDataJson_998.put("screen_x", values[9]);
//					dmpDataJson_998.put("screen_y", values[10]);
//					dmpDataJson_998.put("pa_event", values[11]);
//					if(values[11].toUpperCase().equals("TRACKING")) {
//						dmpDataJson_998.put("event_id", values[12]);
//						dmpDataJson_998.put("prod_id", values[13]);
//						dmpDataJson_998.put("prod_price", values[14]);
//						dmpDataJson_998.put("prod_dis", values[15]);
//					}else if(values[11].toUpperCase().equals("PAGE_VIEW")) {
//						dmpDataJson_998.put("event_id", "");
//					}else if(values[11].toUpperCase().equals("CONVERT")) {
//						dmpDataJson_998.put("event_id", values[12]);
//					}
//					
//					if(values[5].contains("24h.pchome.com.tw")) {
//						String pageCategory = "";
//						if(values[5].equals("https://24h.pchome.com.tw/") || values[5].contains("htm") || values[5].contains("index") || values[5].contains("?fq=") || values[5].contains("store/?q=")) {
//							return;
//						}else if(values[5].contains("?")) {
//							pageCategory = values[5].substring(0, values[5].indexOf("?"));
//							pageCategory = pageCategory.split("/")[pageCategory.split("/").length - 1];
//						}else {
//							pageCategory = values[5].split("/")[values[5].split("/").length - 1];
//						}
//						dmpDataJson_998.put("op1", pageCategory);
//					}
//				}catch(Exception e) {
//					System.out.println(">>>>pa set json fail:"+e.getMessage());
//					System.out.println(">>>>pa set json fail log size:"+logStr.split(paclSymbol,-1).length);
//					String[] logarray = logStr.split(paclSymbol,-1);
//					for (int i = 0; i < logarray.length; i++) {
//						System.out.println(">>>>pa set json fail:["+i+"]:"+logarray[i]);
//					}
//					System.out.println(">>>>pa set json fail logStr:"+logStr);
//					return;
//				}
			}else if(logpath.contains("bu_log")) {
//				try {
//					String[] values = logStr.split(paclSymbol,-1);
//					if(StringUtils.isBlank(values[2])) {
//						return;
//					}
//					dmpDataJson_998.put("fileName", fileName);
//					dmpDataJson_998.put("log_date", values[0]);
//					dmpDataJson_998.put("uuid", values[2]);
//					if(values[2].contains("xxx-")) {
//						dmpDataJson_998.put("uuid_flag", "y");
//					}else {
//						dmpDataJson_998.put("uuid_flag", "n");
//					}
//					dmpDataJson_998.put("url", values[6]);
//					dmpDataJson_998.put("referer", values[5]);
//					dmpDataJson_998.put("domain", values[7]);
//					dmpDataJson_998.put("log_source", "bu_log");
//					dmpDataJson_998.put("ad_view", 0);
//					dmpDataJson_998.put("vpv", 0);
//					dmpDataJson_998.put("pa_event", "mark");
//					dmpDataJson_998.put("trigger_type", "pv");
//					dmpDataJson_998.put("ck", 0);
//					dmpDataJson_998.put("pv", 1);
//					//地區資訊 [area_info_classify] null:ip不正確,N:ip比對不到
//					dmpDataJson_998.put("ip", values[1]);
//					dmpDataJson_998.put("area_info_source", "ip");
//					//時間資訊
//					//裝置資訊 [device_info_classify] null:user_agent為空
//					dmpDataJson_998.put("user_agent", values[8].replaceAll("\"", ""));
//					dmpDataJson_998.put("pa_id", values[4]);
//					dmpDataJson_998.put("screen_x", values[9]);
//					dmpDataJson_998.put("screen_y", values[10]);
//					dmpDataJson_998.put("event_id", "24h");
//				}catch(Exception e) {
//					System.out.println(">>>>bulog set json fail:"+e.getMessage());
//					System.out.println(">>>>bulog set json fail log size:"+logStr.split(paclSymbol,-1).length);
//					String[] logarray = logStr.split(paclSymbol,-1);
//					for (int i = 0; i < logarray.length; i++) {
//						System.out.println(">>>>bulog set json fail:["+i+"]:"+logarray[i]);
//					}
//					System.out.println(">>>>bulog set json logStr:"+logStr);
//					return;
//				}
			}
//			if(dmpDataJson_998.getAsString("referer").contains("24h.pchome.com.tw")) {
//				String pageCategory = "";
//				if(dmpDataJson_998.getAsString("referer").equals("https://24h.pchome.com.tw/") || dmpDataJson_998.getAsString("referer").contains("htm") || dmpDataJson_998.getAsString("referer").contains("index") || dmpDataJson_998.getAsString("referer").contains("?fq=") || dmpDataJson_998.getAsString("referer").contains("store/?q=")) {
//					return;
//				}else if(dmpDataJson_998.getAsString("referer").contains("?")) {
//					pageCategory = dmpDataJson_998.getAsString("referer").substring(0, dmpDataJson_998.getAsString("referer").indexOf("?"));
//					pageCategory = pageCategory.split("/")[pageCategory.split("/").length - 1];
//				}else {
//					pageCategory = dmpDataJson_998.getAsString("referer").split("/")[dmpDataJson_998.getAsString("referer").split("/").length - 1];
//				}
//				dmpDataJson_998.put("mark_value", pageCategory);
//			}
//			
//			
			//開始DMP資訊
			//1.地區處理元件(ip 轉國家、城市)
			try {
				geoIpComponent.ipTransformGEO(dmpDataJson_998);
			}catch(Exception e) {
				System.out.println(">>>>process source area fail:"+e.getMessage());
				System.out.println(">>>>>>logStr:" +logStr);
				System.out.println(">>>>>>fileName:" +fileName);
				return;
			}
////			//2.時間處理元件(日期時間字串轉成小時)	
////			try {
////				dateTimeComponent.datetimeTransformHour(dmpDataJson); 
////			}catch(Exception e) {
////				log.error(">>>>process source time fail:"+e.getMessage());
////				log.error(">>>>>>logStr:" +logStr);
////				log.error(">>>>>>fileName:" +fileName);
////				return;
////			}
			//3.裝置處理元件(UserAgent轉成裝置資訊)
			try {
				deviceComponent.parseUserAgentToDevice(dmpDataJson_998);
			}catch(Exception e) {
				System.out.println(">>>>process source device fail:"+e.getMessage());
				System.out.println(">>>>>>logStr:" +logStr);
				System.out.println(">>>>>>fileName:" +fileName);
				return;
			}
//			//4.分類處理元件(分析click、24H、Ruten、campaign分類)
//			try {
//				if ((dmpDataJson_998.getAsString("trigger_type").equals("ck") || dmpDataJson_998.getAsString("log_source").equals("campaign")) ) {// kdcl ad_click的adclass  或   campaign log的adclass 	//&& StringUtils.isNotBlank(dmpLogBeanResult.getAdClass())
//					try {
//						DmpLogMapper.aCategoryLogDataClick.processCategory(dmpDataJson_998, null);
//					}catch(Exception e) {
//						System.out.println(">>>>process source ck_campaign fail:"+e.getMessage());
//						System.out.println(">>>>>>logStr:" +logStr);
//						System.out.println(">>>>>>fileName:" +fileName);
//						return;
//					}
//				}else if (dmpDataJson_998.getAsString("trigger_type").equals("pv") && StringUtils.isNotBlank(dmpDataJson_998.getAsString("referer")) && dmpDataJson_998.getAsString("referer").contains("ruten")) {	// 露天
//					try {
//						DmpLogMapper.aCategoryLogDataRetun.processCategory(dmpDataJson_998, dBCollection_class_url);
//					}catch(Exception e) {
//						System.out.println(">>>>process source pv_ruten fail:"+e.getMessage());
//						System.out.println(">>>>>>logStr:" +logStr);
//						System.out.println(">>>>>>fileName:" +fileName);
//						return;
//					}
//				}else if (dmpDataJson_998.getAsString("trigger_type").equals("pv") && StringUtils.isNotBlank(dmpDataJson_998.getAsString("referer")) && dmpDataJson_998.getAsString("referer").contains("24h")) {		// 24h
//					try {
//						DmpLogMapper.aCategoryLogData24H.processCategory(dmpDataJson_998, dBCollection_class_url);
//					}catch(Exception e) {
//						System.out.println(">>>>process source pv_24h fail:"+e.getMessage());
//						System.out.println(">>>>>>logStr:" +logStr);
//						System.out.println(">>>>>>dmpDataJson:" +dmpDataJson_998);
//						System.out.println(">>>>>>fileName:" +fileName);
//						return;
//					}
//				}
//			}catch(Exception e) {
//				System.out.println(">>>>process source class type fail:"+e.getMessage());
//				System.out.println(">>>>>>logStr:" +logStr);
//				System.out.println(">>>>>>dmpDataJson:" +dmpDataJson_998);
//				System.out.println(">>>>>>fileName:" +fileName);
//				return;
//			}
//				
//			
//			//館別分類
//			if(logpath.contains("bu_log")) {
//				try {
//					if(StringUtils.isNotBlank(dmpDataJson_998.getAsString("mark_value"))) {
//						process24CategoryLevel(dmpDataJson_998);
//					}
//				}catch(Exception e) {
//					System.out.println(">>>>>>>fail process 24 category level:"+e.getMessage());
//					return;
//				}
//			}
//			
////			寫入reduce
//			try {
//				context.write(new Text(dmpDataJson.getAsString("uuid")), new Text(dmpDataJson.toString()));	
//			} catch (Exception e) {
//				log.error(">>>>write to reduce fail:"+e.getMessage());
//			}
	}
	
	
	//處理24館別階層
	private void process24CategoryLevel(net.minidev.json.JSONObject dmpDataJson_998) throws Exception{
//		String markValue = dmpDataJson_998.getAsString("mark_value");
//		int level = 0;
//		if(markValue.length() == 4) {
//			level = 2;
//		}
//		if(markValue.length() == 6) {
//			level = 3;
//		}
//		if(categoryLevelMappingMap.containsKey(markValue)) {
//			JSONObject layerJson = categoryLevelMappingMap.get(markValue);
////			Iterator<String> keys = layerJson.keys();
////			while(keys.hasNext()) {
////			    String key = keys.next();
////			    String value = layerJson.getString(key);
////			    dmpDataJson.put(key, value);
////			}
//		}else {
//			for (String string : categoryLevelMappingList) {
//				String level1 = string.split("<PCHOME>")[0];
//				String level2 = string.split("<PCHOME>")[1];
//				String level3 = string.split("<PCHOME>")[2];
//				if(level1.equals(markValue)) {
//					dmpDataJson_998.put("mark_layer1", "1");
//					dmpDataJson_998.put("mark_value1", level1);
//					JSONObject layerJson = new JSONObject();
//					layerJson.put("mark_layer1", "1");
//					layerJson.put("mark_value1", level1);
//					categoryLevelMappingMap.put(markValue, layerJson);
//					break;
//				}else if(level2.equals(markValue)) {
//					dmpDataJson_998.put("mark_layer1", "1");
//					dmpDataJson_998.put("mark_value1", level1);
//					dmpDataJson_998.put("mark_layer2", "2");
//					dmpDataJson_998.put("mark_value2", level2);
//					JSONObject layerJson = new JSONObject();
//					layerJson.put("mark_layer1", "1");
//					layerJson.put("mark_value1", level1);
//					layerJson.put("mark_layer2", "2");
//					layerJson.put("mark_value2", level2);
//					categoryLevelMappingMap.put(markValue, layerJson);
//					break;
//				}else if(level3.equals(markValue)) {
//					dmpDataJson_998.put("mark_layer1", "1");
//					dmpDataJson_998.put("mark_value1", level1);
//					dmpDataJson_998.put("mark_layer2", "2");
//					dmpDataJson_998.put("mark_value2", level2);
//					dmpDataJson_998.put("mark_layer3", "3");
//					dmpDataJson_998.put("mark_value3", level3);
//					JSONObject layerJson = new JSONObject();
//					layerJson.put("mark_layer1", "1");
//					layerJson.put("mark_value1", level1);
//					layerJson.put("mark_layer2", "2");
//					layerJson.put("mark_value2", level2);
//					layerJson.put("mark_layer3", "3");
//					layerJson.put("mark_value3", level3);
//					categoryLevelMappingMap.put(markValue, layerJson);
//					break;
//				}
//			}
//		}
	}
//	
//	
//	public DmpLogBean dmpBeanIntegrate(DmpLogBean dmpLogBeanResult) throws Exception {
//		dmpLogBeanResult.setMemid( StringUtils.isBlank(dmpLogBeanResult.getMemid()) ? "null" : dmpLogBeanResult.getMemid());
//		dmpLogBeanResult.setUuid( StringUtils.isBlank(dmpLogBeanResult.getUuid()) ? "null" : dmpLogBeanResult.getUuid());
//		dmpLogBeanResult.setCategory( StringUtils.isBlank(dmpLogBeanResult.getCategory()) ? "null" : dmpLogBeanResult.getCategory());
//		dmpLogBeanResult.setCategorySource( StringUtils.isBlank(dmpLogBeanResult.getCategorySource()) ? "null" : dmpLogBeanResult.getCategorySource());
//		dmpLogBeanResult.setSex( StringUtils.isBlank(dmpLogBeanResult.getSex()) ? "null" : dmpLogBeanResult.getSex());
//		dmpLogBeanResult.setSexSource( StringUtils.isBlank(dmpLogBeanResult.getSexSource()) ? "null" : dmpLogBeanResult.getSexSource());
//		dmpLogBeanResult.setAge( StringUtils.isBlank(dmpLogBeanResult.getAge()) ? "null" : dmpLogBeanResult.getAge());
//		dmpLogBeanResult.setAgeSource( StringUtils.isBlank(dmpLogBeanResult.getAgeSource()) ? "null" : dmpLogBeanResult.getAgeSource());
//		dmpLogBeanResult.setCountry( StringUtils.isBlank(dmpLogBeanResult.getCountry()) ? "null" : dmpLogBeanResult.getCountry());
//		dmpLogBeanResult.setAreaInfoSource( StringUtils.isBlank(dmpLogBeanResult.getAreaInfoSource()) ? "null" : dmpLogBeanResult.getAreaInfoSource());
//		dmpLogBeanResult.setCity( StringUtils.isBlank(dmpLogBeanResult.getCity()) ? "null" : dmpLogBeanResult.getCity());
//		dmpLogBeanResult.setAreaInfoSource( StringUtils.isBlank(dmpLogBeanResult.getAreaInfoSource()) ? "null" : dmpLogBeanResult.getAreaInfoSource());
//		dmpLogBeanResult.setDeviceInfo( StringUtils.isBlank(dmpLogBeanResult.getDeviceInfo()) ? "null" : dmpLogBeanResult.getDeviceInfo());
//		dmpLogBeanResult.setDeviceInfoSource( StringUtils.isBlank(dmpLogBeanResult.getDeviceInfoSource()) ? "null" : dmpLogBeanResult.getDeviceInfoSource());
//		dmpLogBeanResult.setDevicePhoneInfo( StringUtils.isBlank(dmpLogBeanResult.getDevicePhoneInfo()) ? "null" : dmpLogBeanResult.getDevicePhoneInfo());
//		dmpLogBeanResult.setDeviceOsInfo( StringUtils.isBlank(dmpLogBeanResult.getDeviceOsInfo()) ? "null" : dmpLogBeanResult.getDeviceOsInfo());
//		dmpLogBeanResult.setDeviceBrowserInfo( StringUtils.isBlank(dmpLogBeanResult.getDeviceBrowserInfo()) ? "null" : dmpLogBeanResult.getDeviceBrowserInfo());
//		dmpLogBeanResult.setHour( StringUtils.isBlank(dmpLogBeanResult.getHour()) ? "null" : dmpLogBeanResult.getHour());
//		dmpLogBeanResult.setTimeInfoSource( StringUtils.isBlank(dmpLogBeanResult.getTimeInfoSource()) ? "null" : dmpLogBeanResult.getTimeInfoSource());
//		dmpLogBeanResult.setPersonalInfoApiClassify( StringUtils.isBlank(dmpLogBeanResult.getPersonalInfoApiClassify()) ? "null" : dmpLogBeanResult.getPersonalInfoApiClassify());
//		dmpLogBeanResult.setPersonalInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getPersonalInfoClassify()) ? "null" : dmpLogBeanResult.getPersonalInfoClassify());
//		dmpLogBeanResult.setClassAdClickClassify( StringUtils.isBlank(dmpLogBeanResult.getClassAdClickClassify()) ? "null" : dmpLogBeanResult.getClassAdClickClassify());
//		dmpLogBeanResult.setClass24hUrlClassify( StringUtils.isBlank(dmpLogBeanResult.getClass24hUrlClassify()) ? "null" : dmpLogBeanResult.getClass24hUrlClassify());
//		dmpLogBeanResult.setClassRutenUrlClassify( StringUtils.isBlank(dmpLogBeanResult.getClassRutenUrlClassify()) ? "null" : dmpLogBeanResult.getClassRutenUrlClassify());
//		dmpLogBeanResult.setAreaInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getAreaInfoClassify()) ? "null" : dmpLogBeanResult.getAreaInfoClassify());
//		dmpLogBeanResult.setDeviceInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getDeviceInfoClassify()) ? "null" : dmpLogBeanResult.getDeviceInfoClassify());
//		dmpLogBeanResult.setTimeInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getTimeInfoClassify()) ? "null" : dmpLogBeanResult.getTimeInfoClassify());
//		return dmpLogBeanResult;
//	}
	
	
//	public String dmpBeanToKafkaJson(DmpLogBean dmpLogBeanResult) throws Exception {
//		recordCount = recordCount + 1;
//		//send kafka key
//		JSONObject keyJson = new JSONObject();
//		keyJson.put("memid", dmpLogBeanResult.getMemid());
//		keyJson.put("uuid", dmpLogBeanResult.getUuid());
//		
//		//send kafka data
//		//category_info
//		JSONObject categoryInfoJson = new JSONObject();
//		categoryInfoJson.put("value", dmpLogBeanResult.getCategory());
//		categoryInfoJson.put("source", dmpLogBeanResult.getCategorySource());
//		
//		//sex_info
//		JSONObject sexInfoJson = new JSONObject();
//		sexInfoJson.put("value", dmpLogBeanResult.getSex());
//		sexInfoJson.put("source", dmpLogBeanResult.getSexSource());
//		
//		//age_info
//		JSONObject ageInfoJson = new JSONObject();
//		ageInfoJson.put("value", dmpLogBeanResult.getAge());
//		ageInfoJson.put("source", dmpLogBeanResult.getAgeSource());
//		
//		//area_country_info
//		JSONObject areaCountryInfoJson = new JSONObject();
//		areaCountryInfoJson.put("value", dmpLogBeanResult.getCountry());
//		areaCountryInfoJson.put("source", dmpLogBeanResult.getAreaInfoSource());
//
//		//area_city_info
//		JSONObject areaCityInfoJson = new JSONObject();
//		areaCityInfoJson.put("value", dmpLogBeanResult.getCity());
//		areaCityInfoJson.put("source", dmpLogBeanResult.getAreaInfoSource());
//					
//		//device_info
//		JSONObject deviceInfoJson = new JSONObject();
//		deviceInfoJson.put("value", dmpLogBeanResult.getDeviceInfo());
//		deviceInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
//		
//		//device_phone_info
//		JSONObject devicePhoneInfoJson = new JSONObject();
//		devicePhoneInfoJson.put("value", dmpLogBeanResult.getDevicePhoneInfo());
//		devicePhoneInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
//		
//		//device_os_info
//		JSONObject deviceOsInfoJson = new JSONObject();
//		deviceOsInfoJson.put("value", dmpLogBeanResult.getDeviceOsInfo());
//		deviceOsInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
//		
//		//device_browser_info
//		JSONObject deviceBrowserInfoJson = new JSONObject();
//		deviceBrowserInfoJson.put("value", dmpLogBeanResult.getDeviceBrowserInfo());
//		deviceBrowserInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
//		
//		//time_info
//		JSONObject timeInfoJson = new JSONObject();
//		timeInfoJson.put("value", dmpLogBeanResult.getHour());
//		timeInfoJson.put("source", dmpLogBeanResult.getTimeInfoSource());
//
//		//put classify Array
//		JSONArray classifyArray = new JSONArray();
//		//kdcl classify 
//		if ( (StringUtils.equals(dmpLogBeanResult.getSource(), "kdcl")) ){
//			
//			//memid_kdcl_log_personal_info_api
//			JSONObject memid_kdcl_log_personal_info_api = new JSONObject();
//			memid_kdcl_log_personal_info_api.put("memid_kdcl_log_personal_info_api", dmpLogBeanResult.getPersonalInfoApiClassify());
//			classifyArray.put(memid_kdcl_log_personal_info_api);
//			
//			//all_kdcl_log_personal_info
//			JSONObject all_kdcl_log_personal_info = new JSONObject();
//			all_kdcl_log_personal_info.put("all_kdcl_log_personal_info", dmpLogBeanResult.getPersonalInfoClassify());
//			classifyArray.put(all_kdcl_log_personal_info);
//			
//			//all_kdcl_log_class_ad_click
//			JSONObject all_kdcl_log_class_ad_click = new JSONObject();
//			all_kdcl_log_class_ad_click.put("all_kdcl_log_class_ad_click", dmpLogBeanResult.getClassAdClickClassify());
//			classifyArray.put(all_kdcl_log_class_ad_click);
//			
//			//all_kdcl_log_class_24h_url
//			JSONObject all_kdcl_log_class_24h_url = new JSONObject();
//			all_kdcl_log_class_24h_url.put("all_kdcl_log_class_24h_url", dmpLogBeanResult.getClass24hUrlClassify());
//			classifyArray.put(all_kdcl_log_class_24h_url);
//			
//			//all_kdcl_log_class_ruten_url
//			JSONObject all_kdcl_log_class_ruten_url = new JSONObject();
//			all_kdcl_log_class_ruten_url.put("all_kdcl_log_class_ruten_url", dmpLogBeanResult.getClassRutenUrlClassify());
//			classifyArray.put(all_kdcl_log_class_ruten_url);
//			
//			//all_kdcl_log_area_info
//			JSONObject all_kdcl_log_area_info = new JSONObject();
//			all_kdcl_log_area_info.put("all_kdcl_log_area_info", dmpLogBeanResult.getAreaInfoClassify());
//			classifyArray.put(all_kdcl_log_area_info);
//			
//			//all_kdcl_log_device_info
//			JSONObject all_kdcl_log_device_info = new JSONObject();
//			all_kdcl_log_device_info.put("all_kdcl_log_device_info", dmpLogBeanResult.getDeviceInfoClassify());
//			classifyArray.put(all_kdcl_log_device_info);
//			
//			//all_kdcl_log_time_info
//			JSONObject all_kdcl_log_time_info = new JSONObject();
//			all_kdcl_log_time_info.put("all_kdcl_log_time_info", dmpLogBeanResult.getTimeInfoClassify());
//			classifyArray.put(all_kdcl_log_time_info);
//		}
//		
//		//campaign classify
//		if ( (StringUtils.equals(dmpLogBeanResult.getSource(), "campaign")) ){
//			//memid_camp_log_personal_info_api
//			JSONObject memid_camp_log_personal_info_api = new JSONObject();
//			memid_camp_log_personal_info_api.put("memid_camp_log_personal_info_api", dmpLogBeanResult.getPersonalInfoApiClassify());
//			classifyArray.put(memid_camp_log_personal_info_api);
//			
//			//all_camp_log_personal_info
//			JSONObject all_camp_log_personal_info = new JSONObject();
//			all_camp_log_personal_info.put("all_camp_log_personal_info", dmpLogBeanResult.getPersonalInfoClassify());
//			classifyArray.put(all_camp_log_personal_info);
//			
//			//all_camp_log_class_ad_click
//			JSONObject all_camp_log_class_ad_click = new JSONObject();
//			all_camp_log_class_ad_click.put("all_camp_log_class_ad_click", dmpLogBeanResult.getClassAdClickClassify());
//			classifyArray.put(all_camp_log_class_ad_click);
//			
//			//all_camp_log_class_24h_url
//			JSONObject all_camp_log_class_24h_url = new JSONObject();
//			all_camp_log_class_24h_url.put("all_camp_log_class_24h_url", dmpLogBeanResult.getClass24hUrlClassify());
//			classifyArray.put(all_camp_log_class_24h_url);
//			
//			//all_camp_log_class_ruten_url
//			JSONObject all_camp_log_class_ruten_url = new JSONObject();
//			all_camp_log_class_ruten_url.put("all_camp_log_class_ruten_url", dmpLogBeanResult.getClassRutenUrlClassify());
//			classifyArray.put(all_camp_log_class_ruten_url);
//			
//			//all_camp_log_area_info
//			JSONObject all_camp_log_area_info = new JSONObject();
//			all_camp_log_area_info.put("all_camp_log_area_info", dmpLogBeanResult.getAreaInfoClassify());
//			classifyArray.put(all_camp_log_area_info);
//			
//			//all_camp_log_device_info
//			JSONObject all_camp_log_device_info = new JSONObject();
//			all_camp_log_device_info.put("all_camp_log_device_info",dmpLogBeanResult.getDeviceInfoClassify());
//			classifyArray.put(all_camp_log_device_info);
//
//			//all_camp_log_time_info
//			JSONObject all_camp_log_time_info = new JSONObject();
//			all_camp_log_time_info.put("all_camp_log_time_info",  dmpLogBeanResult.getTimeInfoClassify());
//			classifyArray.put(all_camp_log_time_info);
//		}
//		
//		//dataJson
//		JSONObject dataJson = new JSONObject();
//		dataJson.put("category_info", categoryInfoJson);
//		dataJson.put("sex_info", sexInfoJson);
//		dataJson.put("age_info", ageInfoJson);
//		dataJson.put("area_country_info", areaCountryInfoJson );
//		dataJson.put("area_city_info", areaCityInfoJson );
//		dataJson.put("device_info", deviceInfoJson );
//		dataJson.put("device_phone_info", devicePhoneInfoJson );
//		dataJson.put("device_os_info", deviceOsInfoJson);
//		dataJson.put("device_browser_info", deviceBrowserInfoJson);
//		dataJson.put("time_info", timeInfoJson);
//		dataJson.put("classify", classifyArray);
//		
//		//send Kafka Json
//		JSONObject sendKafkaJson = new JSONObject();
//		sendKafkaJson.put("key", keyJson);
//		sendKafkaJson.put("data", dataJson);
//		sendKafkaJson.put("url",  dmpLogBeanResult.getUrl());
//		sendKafkaJson.put("ip", dmpLogBeanResult.getIp());
//		sendKafkaJson.put("record_date", dmpLogBeanResult.getRecordDate());
//		sendKafkaJson.put("org_source", dmpLogBeanResult.getSource());	//(kdcl、campaign)
//		sendKafkaJson.put("date_time", dmpLogBeanResult.getDateTime());
//		sendKafkaJson.put("user_agent", dmpLogBeanResult.getUserAgent() );
//		sendKafkaJson.put("ad_class", dmpLogBeanResult.getAdClass());
//		sendKafkaJson.put("record_count", recordCount);
//		
//		return sendKafkaJson.toString();
//	}
	
	
	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
	

//	 @Autowired
//	 mongoOrgOperations mongoOrgOperations;
//	 public void test() throws Exception{
//	 System.out.println("BBB");
//	 Query query = new
//	 Query(Criteria.where("_id").is("59404c00e4b0ed734829caf4"));
//	 ClassUrlMongoBean classUrlMongoBean = (ClassUrlMongoBean)
//	 mongoOrgOperations.findOne(query, ClassUrlMongoBean.class);
//	 System.out.println(classUrlMongoBean.getUrl());
//	 System.out.println("AAA");
//	 }

	public static void main(String[] args) throws Exception {
//		 DmpLogMapper dmpLogMapper = new DmpLogMapper();
//		 dmpLogMapper.map(null, null, null);
//
//		System.setProperty("spring.profiles.active", "stg");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		DmpLogMapper dmpLogMapper1 = ctx.getBean(DmpLogMapper.class);
//		
//		dmpLogMapper1.test();
//		dmpLogMapper1.map(null, null, null);

	}
//
}
