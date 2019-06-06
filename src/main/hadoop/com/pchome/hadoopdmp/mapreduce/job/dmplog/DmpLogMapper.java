package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.maxmind.geoip2.DatabaseReader;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.pchome.hadoopdmp.enumerate.CategoryLogEnum;
import com.pchome.hadoopdmp.mapreduce.job.component.DateTimeComponent;
import com.pchome.hadoopdmp.mapreduce.job.component.DeviceComponent;
import com.pchome.hadoopdmp.mapreduce.job.component.GeoIpComponent;
import com.pchome.hadoopdmp.mapreduce.job.component.PersonalInfoComponent;
import com.pchome.hadoopdmp.mapreduce.job.factory.ACategoryLogData;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryCodeBean;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogFactory;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryRutenCodeBean;
import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodborg.MongodbOrgHadoopConfig;
import com.sun.tools.javac.code.Attribute.Array;

@Component
public class DmpLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("DmpLogMapper");
	
	private static int recordCount = 0;
	private static int kdclLogLength = 30;
	private static int campaignLogLength = 9;
	private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	private static String campaignSymbol = ",";

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;
	public static String record_hour;
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();	 //Ruten分類表
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
	public static DateTimeComponent dateTimeComponent = new DateTimeComponent();
	public static DeviceComponent deviceComponent = new DeviceComponent();
	private DB mongoOrgOperations;
	public static DatabaseReader reader = null;
	public static InetAddress ipAddress = null;
	private static DBCollection dBCollection_class_url;
	private static DBCollection dBCollection_user_detail;
	private static InputSplit inputSplit;
	private static int count = 0;
	private static int mapCount = 0;
	
	
	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			record_date = context.getConfiguration().get("job.date");
			record_hour = context.getConfiguration().get("job.hour");
			
			log.info("record_date:"+record_date);
			log.info("record_hour:"+record_hour);
			
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			dBCollection_class_url =  this.mongoOrgOperations.getCollection("class_url");
			dBCollection_user_detail = this.mongoOrgOperations.getCollection("user_detail");
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
			
			
		} catch (Exception e) {
			log.error("Mapper setup error>>>>>> " +e);
		}
	}
	
	
	private static String logStr = "";
	private static net.minidev.json.JSONObject dmpDataJson = new net.minidev.json.JSONObject();
	private static String logpath = "";
	private static Map<String,String> hostNameMap = new HashMap<String,String>();
	@Override
	public void map(LongWritable offset, Text value, Context context) {
			inputSplit = (InputSplit)context.getInputSplit(); 
			logpath = ((FileSplit)inputSplit).getPath().toString();
//			log.info(">>>>>>>>>>>>>>>>>>logpath:"+logpath);
			String fileName = ((FileSplit)inputSplit).getPath().getName();
//			log.info(">>>>>>>>>>>>>>>>>>fileName:"+fileName);
			
			logStr = value.toString();
			//1.判斷log來源為kdcl或bu
			if(logpath.contains("alllog")) {
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
						String[] values = logStr.split(kdclSymbol);
						if (values.length < kdclLogLength) {
							return;
						}
						if ((StringUtils.equals(values[1], "null")||StringUtils.isBlank(values[1]) ) && (StringUtils.equals(values[2], "null")||StringUtils.isBlank(values[2])) ){
							return;
						}
						if (StringUtils.isBlank(values[4]) || !(values[4].contains("http"))) {
							return;
						}
						dmpDataJson.put("fileName", fileName);
						dmpDataJson.put("date", values[0]);
						dmpDataJson.put("hour", record_hour);
						dmpDataJson.put("memid", values[1]);
						dmpDataJson.put("uuid", values[2]);
						if(values[2].contains("xxx-")) {
							dmpDataJson.put("uuid_flag", "y");
						}else {
							dmpDataJson.put("uuid_flag", "n");
						}
						dmpDataJson.put("url", "");
						dmpDataJson.put("referer", values[4]);
						dmpDataJson.put("domain", "");
						try {
							if(!hostNameMap.containsKey(values[4].toString())) {
								URI uri = new URI(values[4]);
								String domain = uri.getHost();
								dmpDataJson.put("domain", domain.startsWith("www.") ? domain.substring(4) : domain);
								hostNameMap.put(values[4].toString(), domain.startsWith("www.") ? domain.substring(4) : domain);
							}else {
								dmpDataJson.put("domain", hostNameMap.get(values[4].toString()));
							}
						}catch(Exception e) {
							log.error("*****"+values[4]);
							log.error("*****"+values[2]);
							log.error("*****"+fileName);
							log.error("*****"+record_hour);
							log.error("*****"+record_date);
							log.error(e.getMessage());
						}
						
						dmpDataJson.put("log_source", "kdcl");
						dmpDataJson.put("pfd_customer_info_id", values[24]);
						dmpDataJson.put("pfp_customer_info_id", values[6]);
						dmpDataJson.put("style_id", values[7]);
						dmpDataJson.put("action_id", values[21]);
						dmpDataJson.put("group_id", values[22]);
						dmpDataJson.put("ad_id", values[11]);
						dmpDataJson.put("pfbx_customer_info_id", values[25]);
						dmpDataJson.put("pfbx_position_id", values[26]);
						dmpDataJson.put("ad_view", values[45]);
						dmpDataJson.put("vpv", values[46]);
						dmpDataJson.put("pa_id", "");
						dmpDataJson.put("screen_x", "");
						dmpDataJson.put("screen_y", "");
						dmpDataJson.put("pa_event", "");
						dmpDataJson.put("event_id", "");
						dmpDataJson.put("op1", "");
						dmpDataJson.put("op2", "");
						dmpDataJson.put("email", "");
						dmpDataJson.put("trigger_type", values[13]);
						dmpDataJson.put("ad_class", values[15]);
						
						//地區資訊 [area_info_classify] null:ip不正確,N:ip比對不到
						dmpDataJson.put("ip", values[3]);
						dmpDataJson.put("area_country", "");
						dmpDataJson.put("area_city", "");
						dmpDataJson.put("area_info_source", "ip");
						dmpDataJson.put("area_info_classify", "");
						//時間資訊
						dmpDataJson.put("time_info_source", "kdcl");
						dmpDataJson.put("time_info_classify", "Y");
						//裝置資訊 [device_info_classify] null:user_agent為空
						dmpDataJson.put("user_agent", values[5]);
						dmpDataJson.put("device_info", "");
						dmpDataJson.put("device_phone_info", "");
						dmpDataJson.put("device_os_info", "");
						dmpDataJson.put("device_browser_info", "");
						dmpDataJson.put("device_info_source", "");
						dmpDataJson.put("device_info_classify", "");
						//分類資訊
						dmpDataJson.put("category", "");
						dmpDataJson.put("class_adclick_classify", "");
						dmpDataJson.put("category_source", "");
						//年齡性別資訊
						dmpDataJson.put("sex", "");
						dmpDataJson.put("sex_source", "");
						dmpDataJson.put("age", "");
						dmpDataJson.put("age_source", "");
						//pacl才有欄位
						dmpDataJson.put("pa_id", "");
						dmpDataJson.put("screen_x", "");
						dmpDataJson.put("screen_y", "");
						dmpDataJson.put("pa_event", "");
						dmpDataJson.put("event_id", "");
						dmpDataJson.put("op1", "");
						dmpDataJson.put("op2", "");
						dmpDataJson.put("email", "");
					}else {
						return;
					}
				}catch(Exception e) {
					log.error(">>>>kdcl set json fail");
				}
			}else if(logpath.contains("bulog")) {
				String[] values = logStr.split(paclSymbol);
				try {
					if(values[2].equals("xxx-c33458c6-23b4-4301-b873-c1287b47deea")) {
						log.info("****************************xxx-c33458c6-23b4-4301-b873-c1287b47deea UUID:"+values[2]);
					}
					dmpDataJson.put("fileName", fileName);
					dmpDataJson.put("date", values[0]);
					dmpDataJson.put("hour", record_hour);
					dmpDataJson.put("memid","");
					dmpDataJson.put("uuid", values[2]);
					if(values[2].contains("xxx-")) {
						dmpDataJson.put("uuid_flag", "y");
					}else {
						dmpDataJson.put("uuid_flag", "n");
					}
					dmpDataJson.put("url", values[6]);
					dmpDataJson.put("referer", values[5]);
					dmpDataJson.put("domain", values[7]);
					dmpDataJson.put("log_source", "bulog");
					dmpDataJson.put("pfd_customer_info_id", "");
					dmpDataJson.put("pfp_customer_info_id", "");
					dmpDataJson.put("style_id", "");
					dmpDataJson.put("action_id", "");
					dmpDataJson.put("group_id", "");
					dmpDataJson.put("ad_id", "");
					dmpDataJson.put("pfbx_customer_info_id", "");
					dmpDataJson.put("pfbx_position_id", "");
					dmpDataJson.put("ad_view", "");
					dmpDataJson.put("vpv", "");
					dmpDataJson.put("pa_id", "");
					dmpDataJson.put("screen_x", "");
					dmpDataJson.put("screen_y", "");
					dmpDataJson.put("pa_event", "");
					dmpDataJson.put("event_id", "");
					dmpDataJson.put("op1", "");
					dmpDataJson.put("op2", "");
					dmpDataJson.put("email", "");
					dmpDataJson.put("trigger_type", "pv");
					dmpDataJson.put("ad_class", "");
					
					//地區資訊 [area_info_classify] null:ip不正確,N:ip比對不到
					dmpDataJson.put("ip", values[1]);
					dmpDataJson.put("area_country", "");
					dmpDataJson.put("area_city", "");
					dmpDataJson.put("area_info_source", "ip");
					dmpDataJson.put("area_info_classify", "");
					//時間資訊
					dmpDataJson.put("time_info_source", "bulog");
					dmpDataJson.put("time_info_classify", "Y");
					//裝置資訊 [device_info_classify] null:user_agent為空
					dmpDataJson.put("user_agent", values[8]);
					dmpDataJson.put("device_info", "");
					dmpDataJson.put("device_phone_info", "");
					dmpDataJson.put("device_os_info", "");
					dmpDataJson.put("device_browser_info", "");
					dmpDataJson.put("device_info_source", "");
					dmpDataJson.put("device_info_classify", "");
					//分類資訊
					dmpDataJson.put("category", "");
					dmpDataJson.put("class_adclick_classify", "");
					dmpDataJson.put("category_source", "");
					//年齡性別資訊
					dmpDataJson.put("sex", "");
					dmpDataJson.put("sex_source", "");
					dmpDataJson.put("age", "");
					dmpDataJson.put("age_source", "");
					
					dmpDataJson.put("pa_id", values[4]);
					dmpDataJson.put("screen_x", values[9]);
					dmpDataJson.put("screen_y", values[10]);
					dmpDataJson.put("pa_event", values[11]);
					if(values[11].toUpperCase().equals("TRACKING")) {
						//pa_event:tracking length 13
						dmpDataJson.put("event_id", values[12]);
					}
					if(values[11].toUpperCase().equals("PAGE_VIEW")) {
						dmpDataJson.put("event_id", "");
					}
					if(values[11].toUpperCase().equals("CONVERT")) {
						dmpDataJson.put("event_id", values[12]);
					}
					
					if(values[5].contains("24h.pchome.com.tw")) {
						String pageCategory = "";
						if(values[5].equals("https://24h.pchome.com.tw/") || values[5].contains("htm") || values[5].contains("index") || values[5].contains("?fq=") || values[5].contains("store/?q=")) {
							return;
						}else if(values[5].contains("?")) {
							pageCategory = values[5].split("/")[values[5].split("/").length - 1];
							pageCategory = pageCategory.substring(0, pageCategory.indexOf("?"));
						}else {
							pageCategory = values[5].split("/")[values[5].split("/").length - 1];
						}
						dmpDataJson.put("op1", pageCategory);
					}else {
						dmpDataJson.put("op1", "");
					}
					
					dmpDataJson.put("op2", "");
					
					dmpDataJson.put("email", "");
				}catch(Exception e) {
					log.error(">>>>bulog set json fail values length:"+values.length);
					log.error(">>>>bulog set json fail:"+e.getMessage());
					return;
				}
			}
			//開始DMP資訊
			try {
				//1.地區處理元件(ip 轉國家、城市)
				geoIpComponent.ipTransformGEO(dmpDataJson);
				//2.時間處理元件(日期時間字串轉成小時)		
				dateTimeComponent.datetimeTransformHour(dmpDataJson); 
				//3.裝置處理元件(UserAgent轉成裝置資訊)
				deviceComponent.parseUserAgentToDevice(dmpDataJson);
				//5.分類處理元件(分析click、24H、Ruten、campaign分類)
				if ((dmpDataJson.getAsString("trigger_type").equals("ck") || dmpDataJson.getAsString("log_source").equals("campaign")) ) {// kdcl ad_click的adclass  或   campaign log的adclass 	//&& StringUtils.isNotBlank(dmpLogBeanResult.getAdClass())
					ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
					aCategoryLogData.processCategory(dmpDataJson, null);
				}else if (dmpDataJson.getAsString("trigger_type").equals("pv") && StringUtils.isNotBlank(dmpDataJson.getAsString("referer")) && dmpDataJson.getAsString("referer").contains("ruten")) {	// 露天
					ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
					aCategoryLogData.processCategory(dmpDataJson, dBCollection_class_url);
				}else if (dmpDataJson.getAsString("trigger_type").equals("pv") && StringUtils.isNotBlank(dmpDataJson.getAsString("referer")) && dmpDataJson.getAsString("referer").contains("24h")) {		// 24h
//					ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
//					aCategoryLogData.processCategory(dmpDataJson, dBCollection_class_url);
				}else if (dmpDataJson.getAsString("trigger_type").equals("pv") ){
					dmpDataJson.put("category", "");
					dmpDataJson.put("category_source", "");
					dmpDataJson.put("class_adclick_classify", "");
					dmpDataJson.put("class_24h_url_classify", "");
					dmpDataJson.put("class_ruten_url_classify", "");
				}
			}catch(Exception e) {
				log.error(">>>>process source fail");
				log.error(">>>>>>logStr:" +logStr);
				log.error(">>>>>>fileName:" +fileName);
				return;
			}
			
			//寫入reduce
			try {
				keyOut.set(dmpDataJson.getAsString("uuid"));
				context.write(keyOut, new Text(dmpDataJson.toString()));
				
				//6.個資
//				personalInfoComponent.processPersonalInfo(dmpDataJson, dBCollection_user_detail);
				if(count == 0 && dmpDataJson.getAsString("log_source").equals("bulog")) {
					count = count + 1;
					log.info("****bulog after****:"+dmpDataJson);
					
				}
				if(mapCount == 0 && dmpDataJson.getAsString("log_source").equals("kdcl")) {
					mapCount = mapCount + 1;
					log.info("****kdcl after****:"+dmpDataJson);
				}
				
			} catch (IOException | InterruptedException e) {
				log.error(">>>>write to reduce fail:"+e.getMessage());
			}
			
	}
	
	public DmpLogBean dmpBeanIntegrate(DmpLogBean dmpLogBeanResult) throws Exception {
		dmpLogBeanResult.setMemid( StringUtils.isBlank(dmpLogBeanResult.getMemid()) ? "null" : dmpLogBeanResult.getMemid());
		dmpLogBeanResult.setUuid( StringUtils.isBlank(dmpLogBeanResult.getUuid()) ? "null" : dmpLogBeanResult.getUuid());
		dmpLogBeanResult.setCategory( StringUtils.isBlank(dmpLogBeanResult.getCategory()) ? "null" : dmpLogBeanResult.getCategory());
		dmpLogBeanResult.setCategorySource( StringUtils.isBlank(dmpLogBeanResult.getCategorySource()) ? "null" : dmpLogBeanResult.getCategorySource());
		dmpLogBeanResult.setSex( StringUtils.isBlank(dmpLogBeanResult.getSex()) ? "null" : dmpLogBeanResult.getSex());
		dmpLogBeanResult.setSexSource( StringUtils.isBlank(dmpLogBeanResult.getSexSource()) ? "null" : dmpLogBeanResult.getSexSource());
		dmpLogBeanResult.setAge( StringUtils.isBlank(dmpLogBeanResult.getAge()) ? "null" : dmpLogBeanResult.getAge());
		dmpLogBeanResult.setAgeSource( StringUtils.isBlank(dmpLogBeanResult.getAgeSource()) ? "null" : dmpLogBeanResult.getAgeSource());
		dmpLogBeanResult.setCountry( StringUtils.isBlank(dmpLogBeanResult.getCountry()) ? "null" : dmpLogBeanResult.getCountry());
		dmpLogBeanResult.setAreaInfoSource( StringUtils.isBlank(dmpLogBeanResult.getAreaInfoSource()) ? "null" : dmpLogBeanResult.getAreaInfoSource());
		dmpLogBeanResult.setCity( StringUtils.isBlank(dmpLogBeanResult.getCity()) ? "null" : dmpLogBeanResult.getCity());
		dmpLogBeanResult.setAreaInfoSource( StringUtils.isBlank(dmpLogBeanResult.getAreaInfoSource()) ? "null" : dmpLogBeanResult.getAreaInfoSource());
		dmpLogBeanResult.setDeviceInfo( StringUtils.isBlank(dmpLogBeanResult.getDeviceInfo()) ? "null" : dmpLogBeanResult.getDeviceInfo());
		dmpLogBeanResult.setDeviceInfoSource( StringUtils.isBlank(dmpLogBeanResult.getDeviceInfoSource()) ? "null" : dmpLogBeanResult.getDeviceInfoSource());
		dmpLogBeanResult.setDevicePhoneInfo( StringUtils.isBlank(dmpLogBeanResult.getDevicePhoneInfo()) ? "null" : dmpLogBeanResult.getDevicePhoneInfo());
		dmpLogBeanResult.setDeviceOsInfo( StringUtils.isBlank(dmpLogBeanResult.getDeviceOsInfo()) ? "null" : dmpLogBeanResult.getDeviceOsInfo());
		dmpLogBeanResult.setDeviceBrowserInfo( StringUtils.isBlank(dmpLogBeanResult.getDeviceBrowserInfo()) ? "null" : dmpLogBeanResult.getDeviceBrowserInfo());
		dmpLogBeanResult.setHour( StringUtils.isBlank(dmpLogBeanResult.getHour()) ? "null" : dmpLogBeanResult.getHour());
		dmpLogBeanResult.setTimeInfoSource( StringUtils.isBlank(dmpLogBeanResult.getTimeInfoSource()) ? "null" : dmpLogBeanResult.getTimeInfoSource());
		dmpLogBeanResult.setPersonalInfoApiClassify( StringUtils.isBlank(dmpLogBeanResult.getPersonalInfoApiClassify()) ? "null" : dmpLogBeanResult.getPersonalInfoApiClassify());
		dmpLogBeanResult.setPersonalInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getPersonalInfoClassify()) ? "null" : dmpLogBeanResult.getPersonalInfoClassify());
		dmpLogBeanResult.setClassAdClickClassify( StringUtils.isBlank(dmpLogBeanResult.getClassAdClickClassify()) ? "null" : dmpLogBeanResult.getClassAdClickClassify());
		dmpLogBeanResult.setClass24hUrlClassify( StringUtils.isBlank(dmpLogBeanResult.getClass24hUrlClassify()) ? "null" : dmpLogBeanResult.getClass24hUrlClassify());
		dmpLogBeanResult.setClassRutenUrlClassify( StringUtils.isBlank(dmpLogBeanResult.getClassRutenUrlClassify()) ? "null" : dmpLogBeanResult.getClassRutenUrlClassify());
		dmpLogBeanResult.setAreaInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getAreaInfoClassify()) ? "null" : dmpLogBeanResult.getAreaInfoClassify());
		dmpLogBeanResult.setDeviceInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getDeviceInfoClassify()) ? "null" : dmpLogBeanResult.getDeviceInfoClassify());
		dmpLogBeanResult.setTimeInfoClassify( StringUtils.isBlank(dmpLogBeanResult.getTimeInfoClassify()) ? "null" : dmpLogBeanResult.getTimeInfoClassify());
		
		return dmpLogBeanResult;
	}
	
	
	public String dmpBeanToKafkaJson(DmpLogBean dmpLogBeanResult) throws Exception {
		recordCount = recordCount + 1;
		//send kafka key
		JSONObject keyJson = new JSONObject();
		keyJson.put("memid", dmpLogBeanResult.getMemid());
		keyJson.put("uuid", dmpLogBeanResult.getUuid());
		
		//send kafka data
		//category_info
		JSONObject categoryInfoJson = new JSONObject();
		categoryInfoJson.put("value", dmpLogBeanResult.getCategory());
		categoryInfoJson.put("source", dmpLogBeanResult.getCategorySource());
		
		//sex_info
		JSONObject sexInfoJson = new JSONObject();
		sexInfoJson.put("value", dmpLogBeanResult.getSex());
		sexInfoJson.put("source", dmpLogBeanResult.getSexSource());
		
		//age_info
		JSONObject ageInfoJson = new JSONObject();
		ageInfoJson.put("value", dmpLogBeanResult.getAge());
		ageInfoJson.put("source", dmpLogBeanResult.getAgeSource());
		
		//area_country_info
		JSONObject areaCountryInfoJson = new JSONObject();
		areaCountryInfoJson.put("value", dmpLogBeanResult.getCountry());
		areaCountryInfoJson.put("source", dmpLogBeanResult.getAreaInfoSource());

		//area_city_info
		JSONObject areaCityInfoJson = new JSONObject();
		areaCityInfoJson.put("value", dmpLogBeanResult.getCity());
		areaCityInfoJson.put("source", dmpLogBeanResult.getAreaInfoSource());
					
		//device_info
		JSONObject deviceInfoJson = new JSONObject();
		deviceInfoJson.put("value", dmpLogBeanResult.getDeviceInfo());
		deviceInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
		
		//device_phone_info
		JSONObject devicePhoneInfoJson = new JSONObject();
		devicePhoneInfoJson.put("value", dmpLogBeanResult.getDevicePhoneInfo());
		devicePhoneInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
		
		//device_os_info
		JSONObject deviceOsInfoJson = new JSONObject();
		deviceOsInfoJson.put("value", dmpLogBeanResult.getDeviceOsInfo());
		deviceOsInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
		
		//device_browser_info
		JSONObject deviceBrowserInfoJson = new JSONObject();
		deviceBrowserInfoJson.put("value", dmpLogBeanResult.getDeviceBrowserInfo());
		deviceBrowserInfoJson.put("source", dmpLogBeanResult.getDeviceInfoSource());
		
		//time_info
		JSONObject timeInfoJson = new JSONObject();
		timeInfoJson.put("value", dmpLogBeanResult.getHour());
		timeInfoJson.put("source", dmpLogBeanResult.getTimeInfoSource());

		//put classify Array
		JSONArray classifyArray = new JSONArray();
		//kdcl classify 
		if ( (StringUtils.equals(dmpLogBeanResult.getSource(), "kdcl")) ){
			
			//memid_kdcl_log_personal_info_api
			JSONObject memid_kdcl_log_personal_info_api = new JSONObject();
			memid_kdcl_log_personal_info_api.put("memid_kdcl_log_personal_info_api", dmpLogBeanResult.getPersonalInfoApiClassify());
			classifyArray.put(memid_kdcl_log_personal_info_api);
			
			//all_kdcl_log_personal_info
			JSONObject all_kdcl_log_personal_info = new JSONObject();
			all_kdcl_log_personal_info.put("all_kdcl_log_personal_info", dmpLogBeanResult.getPersonalInfoClassify());
			classifyArray.put(all_kdcl_log_personal_info);
			
			//all_kdcl_log_class_ad_click
			JSONObject all_kdcl_log_class_ad_click = new JSONObject();
			all_kdcl_log_class_ad_click.put("all_kdcl_log_class_ad_click", dmpLogBeanResult.getClassAdClickClassify());
			classifyArray.put(all_kdcl_log_class_ad_click);
			
			//all_kdcl_log_class_24h_url
			JSONObject all_kdcl_log_class_24h_url = new JSONObject();
			all_kdcl_log_class_24h_url.put("all_kdcl_log_class_24h_url", dmpLogBeanResult.getClass24hUrlClassify());
			classifyArray.put(all_kdcl_log_class_24h_url);
			
			//all_kdcl_log_class_ruten_url
			JSONObject all_kdcl_log_class_ruten_url = new JSONObject();
			all_kdcl_log_class_ruten_url.put("all_kdcl_log_class_ruten_url", dmpLogBeanResult.getClassRutenUrlClassify());
			classifyArray.put(all_kdcl_log_class_ruten_url);
			
			//all_kdcl_log_area_info
			JSONObject all_kdcl_log_area_info = new JSONObject();
			all_kdcl_log_area_info.put("all_kdcl_log_area_info", dmpLogBeanResult.getAreaInfoClassify());
			classifyArray.put(all_kdcl_log_area_info);
			
			//all_kdcl_log_device_info
			JSONObject all_kdcl_log_device_info = new JSONObject();
			all_kdcl_log_device_info.put("all_kdcl_log_device_info", dmpLogBeanResult.getDeviceInfoClassify());
			classifyArray.put(all_kdcl_log_device_info);
			
			//all_kdcl_log_time_info
			JSONObject all_kdcl_log_time_info = new JSONObject();
			all_kdcl_log_time_info.put("all_kdcl_log_time_info", dmpLogBeanResult.getTimeInfoClassify());
			classifyArray.put(all_kdcl_log_time_info);
		}
		
		//campaign classify
		if ( (StringUtils.equals(dmpLogBeanResult.getSource(), "campaign")) ){
			//memid_camp_log_personal_info_api
			JSONObject memid_camp_log_personal_info_api = new JSONObject();
			memid_camp_log_personal_info_api.put("memid_camp_log_personal_info_api", dmpLogBeanResult.getPersonalInfoApiClassify());
			classifyArray.put(memid_camp_log_personal_info_api);
			
			//all_camp_log_personal_info
			JSONObject all_camp_log_personal_info = new JSONObject();
			all_camp_log_personal_info.put("all_camp_log_personal_info", dmpLogBeanResult.getPersonalInfoClassify());
			classifyArray.put(all_camp_log_personal_info);
			
			//all_camp_log_class_ad_click
			JSONObject all_camp_log_class_ad_click = new JSONObject();
			all_camp_log_class_ad_click.put("all_camp_log_class_ad_click", dmpLogBeanResult.getClassAdClickClassify());
			classifyArray.put(all_camp_log_class_ad_click);
			
			//all_camp_log_class_24h_url
			JSONObject all_camp_log_class_24h_url = new JSONObject();
			all_camp_log_class_24h_url.put("all_camp_log_class_24h_url", dmpLogBeanResult.getClass24hUrlClassify());
			classifyArray.put(all_camp_log_class_24h_url);
			
			//all_camp_log_class_ruten_url
			JSONObject all_camp_log_class_ruten_url = new JSONObject();
			all_camp_log_class_ruten_url.put("all_camp_log_class_ruten_url", dmpLogBeanResult.getClassRutenUrlClassify());
			classifyArray.put(all_camp_log_class_ruten_url);
			
			//all_camp_log_area_info
			JSONObject all_camp_log_area_info = new JSONObject();
			all_camp_log_area_info.put("all_camp_log_area_info", dmpLogBeanResult.getAreaInfoClassify());
			classifyArray.put(all_camp_log_area_info);
			
			//all_camp_log_device_info
			JSONObject all_camp_log_device_info = new JSONObject();
			all_camp_log_device_info.put("all_camp_log_device_info",dmpLogBeanResult.getDeviceInfoClassify());
			classifyArray.put(all_camp_log_device_info);

			//all_camp_log_time_info
			JSONObject all_camp_log_time_info = new JSONObject();
			all_camp_log_time_info.put("all_camp_log_time_info",  dmpLogBeanResult.getTimeInfoClassify());
			classifyArray.put(all_camp_log_time_info);
		}
		
		//dataJson
		JSONObject dataJson = new JSONObject();
		dataJson.put("category_info", categoryInfoJson);
		dataJson.put("sex_info", sexInfoJson);
		dataJson.put("age_info", ageInfoJson);
		dataJson.put("area_country_info", areaCountryInfoJson );
		dataJson.put("area_city_info", areaCityInfoJson );
		dataJson.put("device_info", deviceInfoJson );
		dataJson.put("device_phone_info", devicePhoneInfoJson );
		dataJson.put("device_os_info", deviceOsInfoJson);
		dataJson.put("device_browser_info", deviceBrowserInfoJson);
		dataJson.put("time_info", timeInfoJson);
		dataJson.put("classify", classifyArray);
		
		//send Kafka Json
		JSONObject sendKafkaJson = new JSONObject();
		sendKafkaJson.put("key", keyJson);
		sendKafkaJson.put("data", dataJson);
		sendKafkaJson.put("url",  dmpLogBeanResult.getUrl());
		sendKafkaJson.put("ip", dmpLogBeanResult.getIp());
		sendKafkaJson.put("record_date", dmpLogBeanResult.getRecordDate());
		sendKafkaJson.put("org_source", dmpLogBeanResult.getSource());	//(kdcl、campaign)
		sendKafkaJson.put("date_time", dmpLogBeanResult.getDateTime());
		sendKafkaJson.put("user_agent", dmpLogBeanResult.getUserAgent() );
		sendKafkaJson.put("ad_class", dmpLogBeanResult.getAdClass());
		sendKafkaJson.put("record_count", recordCount);
		
		return sendKafkaJson.toString();
	}
	
	
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
