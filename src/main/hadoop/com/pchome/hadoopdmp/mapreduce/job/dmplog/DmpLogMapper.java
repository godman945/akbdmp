package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

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

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class DmpLogMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	private static String[] markLevelList = {"mark_layer1","mark_layer2","mark_layer3"};
//	private static String[] markValueList = {"mark_value1","mark_value2","mark_value3"};
	private static Logger log = Logger.getLogger(DmpLogMapper.class);
	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
	public static DeviceComponent deviceComponent = new DeviceComponent();
	public static String record_date;
	public static String record_hour;
	public static DatabaseReader databaseReader = null;
	private static String logpath = "";
	private static String logStr = "";
	private static String[] values = null;
	private static InputSplit inputSplit;
	private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	private static int kdclLogLength = 30;
	private static int campaignLogLength = 9;
	private static Map<String, String> hostNameMap = new HashMap<String, String>();
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>(); // 分類表
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>(); // 分類個資表
	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>(); // 24H分類表
	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();
	public static List<String> categoryLevelMappingList = new ArrayList<String>();
	public static ACategoryLogData aCategoryLogDataClick = null;
	public static ACategoryLogData aCategoryLogDataRetun = null;
	public static ACategoryLogData aCategoryLogData24H = null;
	private DB mongoOrgOperations;
	private static DBCollection dBCollection_class_url;
	public static Map<String, org.json.JSONObject> categoryLevelMappingMap = new HashMap<String, org.json.JSONObject>();

	private static net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
	private static net.minidev.json.JSONObject dmpDataJson999 = new net.minidev.json.JSONObject();
	private static net.minidev.json.JSONObject dmpDataJson = new net.minidev.json.JSONObject();

	
	private static int count = 0;
	public void setup(Context context) {
		System.out.println(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"	+ context.getConfiguration().get("spring.profiles.active"));
		try {
			System.out.println("*********>>>>>>>>>>>:" + sdf.format(new Date()));
			record_date = context.getConfiguration().get("job.date");
			record_hour = context.getConfiguration().get("job.hour");
			System.out.println("record_date:" + record_date);
			System.out.println("record_hour:" + record_hour);
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.aCategoryLogDataClick = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
			this.aCategoryLogDataRetun = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
			this.aCategoryLogData24H = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
//			
			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			dBCollection_class_url = this.mongoOrgOperations.getCollection("class_url");
			// load 推估分類個資表(ClsfyGndAgeCrspTable.txt)
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
					categoryList.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),
							tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
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
				String[] tmpStrAry = line.split(","); // "0001000000000000","電腦、電腦周邊"
				categoryRutenBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
				categoryRutenBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
				categoryRutenBeanList.add(categoryRutenBean);
			}
			// IP轉城市
			File database = new File(path[5].toString());
			databaseReader = new DatabaseReader.Builder(database).build();

			
			// 24館別階層對應表
			FileSystem fs = FileSystem.get(conf);
			org.apache.hadoop.fs.Path category24MappingFile = new org.apache.hadoop.fs.Path("hdfs://druid1.mypchome.com.tw:9000/hadoop_file/24h_menu-1.csv");
			FSDataInputStream inputStream = fs.open(category24MappingFile);
			Reader reader = new InputStreamReader(inputStream);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
			for (CSVRecord csvRecord : csvParser) {
				String data = csvRecord.get(1) + "<PCHOME>" + csvRecord.get(3) + "<PCHOME>" + csvRecord.get(5);
				categoryLevelMappingList.add(data);
			}
			
		} catch (Exception e) {
			System.out.println("Mapper setup error>>>>>> " + e.getMessage());
		}
	}

	public synchronized void map(LongWritable offset, Text value, Context context) {
//		清空mapper中json資料
		inputSplit = (InputSplit) context.getInputSplit();
		logpath = ((FileSplit) inputSplit).getPath().toString();
		String fileName = ((FileSplit) inputSplit).getPath().getName();
		dmpDataJson.clear();
		dmpDataJson.put("fileName", "");
		dmpDataJson.put("log_date", "");
		dmpDataJson.put("hour", "");
		dmpDataJson.put("memid", "");
		dmpDataJson.put("uuid", "");
		dmpDataJson.put("uuid_flag", "");
		dmpDataJson.put("url", "");
		dmpDataJson.put("referer", "");
		dmpDataJson.put("domain", "");
		dmpDataJson.put("log_source", "");
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
		dmpDataJson.put("screen_x", "");
		dmpDataJson.put("screen_y", "");
		dmpDataJson.put("event_id", "");
		dmpDataJson.put("trigger_type", "");
		dmpDataJson.put("ck", 0);
		dmpDataJson.put("pv", 0);
		dmpDataJson.put("ad_class", "");
		dmpDataJson.put("ip", "");
		dmpDataJson.put("area_country", "");
		dmpDataJson.put("area_city", "");
		dmpDataJson.put("area_info_source", "");
		dmpDataJson.put("area_info_classify", "");
		dmpDataJson.put("user_agent", "");
		dmpDataJson.put("device_info", "");
		dmpDataJson.put("device_phone_info", "");
		dmpDataJson.put("device_os_info", "");
		dmpDataJson.put("device_browser_info", "");
		dmpDataJson.put("device_info_source", "");
		dmpDataJson.put("device_info_classify", "");
		dmpDataJson.put("category", "");
		dmpDataJson.put("class_adclick_classify", "");
		dmpDataJson.put("category_source", "");
		dmpDataJson.put("sex", "");
		dmpDataJson.put("sex_source", "");
		dmpDataJson.put("age", "");
		dmpDataJson.put("age_source", "");
		dmpDataJson.put("personal_info_api_classify", "");
		dmpDataJson.put("pa_id", "");
		dmpDataJson.put("screen_x", "");
		dmpDataJson.put("screen_y", "");
		dmpDataJson.put("pa_event", "");
		dmpDataJson.put("event_id", "");
		dmpDataJson.put("roule_id", "");
		dmpDataJson.put("convert_price", "0");
		dmpDataJson.put("convert_num", "0");
		dmpDataJson.put("prod_id", "");
		dmpDataJson.put("prod_price", "");
		dmpDataJson.put("prod_dis", "");
		dmpDataJson.put("op1", "");
		dmpDataJson.put("op2", "");
		dmpDataJson.put("email", "");
		dmpDataJson.put("mark_value", "");
		dmpDataJson.put("mark_layer1", "");
		dmpDataJson.put("mark_layer2", "");
		dmpDataJson.put("mark_layer3", "");
		dmpDataJson.put("mark_layer4", "");

		values = null;
		logStr = "";
		logStr = value.toString();
		count = count + 1;
		if (logpath.contains("kdcl_log")) {
			try {
				// kdcl log raw data格式為一般或是Campaign
				if (logStr.indexOf(kdclSymbol) > -1) {
					// values[0] date time (2018-01-04 04:57:12)
					// values[1] memid
					// values[2] uuid
					// values[3] ip
					// values[4] referer
					// values[5] UserAgent
					// values[13] ck,pv
					// values[15] ad_class
					this.values = this.logStr.split(kdclSymbol, -1);
					if (values.length < kdclLogLength) {
						return;
					}
					if ((StringUtils.equals(values[1], "null") || StringUtils.isBlank(values[1]))
							&& (StringUtils.equals(values[2], "null") || StringUtils.isBlank(values[2]))) {
						return;
					}
					if (StringUtils.isBlank(values[4]) || !(values[4].contains("http"))) {
						return;
					}

					dmpDataJson.put("fileName", fileName);
					dmpDataJson.put("log_date", values[0]);
					dmpDataJson.put("memid", values[1]);
					dmpDataJson.put("uuid", values[2]);
					if (values[2].contains("xxx-")) {
						dmpDataJson.put("uuid_flag", "y");
					} else {
						dmpDataJson.put("uuid_flag", "n");
					}
					dmpDataJson.put("referer", values[4]);
					try {
						if (hostNameMap.containsKey(values[4].toString())) {
							dmpDataJson.put("domain", hostNameMap.get(values[4].toString()));
						} else {
							URI uri = new URI(values[4]);
							String domain = uri.getHost();
							dmpDataJson.put("domain", domain.startsWith("www.") ? domain.substring(4) : domain);
							hostNameMap.put(values[4].toString(),
									domain.startsWith("www.") ? domain.substring(4) : domain);
						}
					} catch (Exception e) {
						System.out.println("kdcl log process domain fail:" + e.getMessage());
						System.out.println("kdcl log process domain fail json:" + dmpDataJson);
						return;
					}
					dmpDataJson.put("log_source", "kdcl_log");
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
					dmpDataJson.put("trigger_type", values[13]);
					if (values[13].toUpperCase().equals("CK")) {
						dmpDataJson.put("ck", 1);
						dmpDataJson.put("pv", 0);
					} else if (values[13].toUpperCase().equals("PV")) {
						dmpDataJson.put("ck", 0);
						dmpDataJson.put("pv", 1);
					}
					dmpDataJson.put("ad_class", values[15]);
					dmpDataJson.put("ip", values[3]);
					dmpDataJson.put("area_info_source", "ip");
					// 裝置資訊 [device_info_classify] null:user_agent為空
					dmpDataJson.put("user_agent", values[5].replaceAll("\"", ""));
					if (values[4].contains("24h.pchome.com.tw")) {
						String pageCategory = "";
						if (values[4].equals("https://24h.pchome.com.tw/") || values[4].contains("htm")
								|| values[4].contains("index") || values[4].contains("?fq=")
								|| values[4].contains("store/?q=")) {
							return;
						} else if (values[4].contains("?")) {
							pageCategory = values[4].split("/")[values[4].split("/").length - 1];
							pageCategory = pageCategory.substring(0, pageCategory.indexOf("?"));
						} else {
							pageCategory = values[4].split("/")[values[4].split("/").length - 1];
						}
						dmpDataJson.put("op1", pageCategory);
					}
				} else {
					return;
				}
			} catch (Exception e) {
				System.out.println(">>>> kdcl set json fail:" + dmpDataJson);
			}
		} else if (logpath.contains("pacl_log")) {
				try {
					this.values = this.logStr.split(paclSymbol,-1);
					dmpDataJson.put("fileName", fileName);
					dmpDataJson.put("log_date", values[0]);
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
					dmpDataJson.put("log_source", "pacl_log");
					dmpDataJson.put("ad_view", 0);
					dmpDataJson.put("vpv", 0);
					dmpDataJson.put("trigger_type", "pv");
					dmpDataJson.put("ck", 0);
					dmpDataJson.put("pv", 1);
					//地區資訊 [area_info_classify] null:ip不正確,N:ip比對不到
					dmpDataJson.put("ip", values[1]);
					dmpDataJson.put("area_info_source", "ip");
					//裝置資訊 [device_info_classify] null:user_agent為空
					dmpDataJson.put("user_agent", values[8].replaceAll("\"", ""));
					dmpDataJson.put("pa_id", values[4]);
					dmpDataJson.put("screen_x", values[9]);
					dmpDataJson.put("screen_y", values[10]);
					dmpDataJson.put("pa_event", values[11]);
					if(values[11].toUpperCase().equals("TRACKING")) {
						dmpDataJson.put("event_id", values[12]);
						dmpDataJson.put("prod_id", values[13]);
						dmpDataJson.put("prod_price", values[14]);
						dmpDataJson.put("prod_dis", values[15]);
					}else if(values[11].toUpperCase().equals("PAGE_VIEW")) {
						dmpDataJson.put("event_id", "");
					}else if(values[11].toUpperCase().equals("CONVERT")) {
						dmpDataJson.put("event_id", values[12]);
						dmpDataJson.put("roule_id", values[13]);
						dmpDataJson.put("convert_price", values[14]);
					}
					
					if(values[5].contains("24h.pchome.com.tw")) {
						String pageCategory = "";
						if(values[5].equals("https://24h.pchome.com.tw/") || values[5].contains("htm") || values[5].contains("index") || values[5].contains("?fq=") || values[5].contains("store/?q=")) {
							return;
						}else if(values[5].contains("?")) {
							pageCategory = values[5].substring(0, values[5].indexOf("?"));
							pageCategory = pageCategory.split("/")[pageCategory.split("/").length - 1];
						}else {
							pageCategory = values[5].split("/")[values[5].split("/").length - 1];
						}
						dmpDataJson.put("op1", pageCategory);
					}
				}catch(Exception e) {
					System.out.println(">>>>pa set json fail:"+e.getMessage());
					System.out.println(">>>>pa set json fail log size:"+logStr.split(paclSymbol,-1).length);
					String[] logarray = logStr.split(paclSymbol,-1);
					for (int i = 0; i < logarray.length; i++) {
						System.out.println(">>>>pa set json fail:["+i+"]:"+logarray[i]);
					}
					System.out.println(">>>>pa set json fail logStr:"+logStr);
					return;
				}
		} else if (logpath.contains("bu_log")) {
			try {
				String[] values = logStr.split(paclSymbol, -1);
				if (StringUtils.isBlank(values[2])) {
					return;
				}
				dmpDataJson.put("fileName", fileName);
				dmpDataJson.put("log_date", values[0]);
				dmpDataJson.put("uuid", values[2]);
				if (values[2].contains("xxx-")) {
					dmpDataJson.put("uuid_flag", "y");
				} else {
					dmpDataJson.put("uuid_flag", "n");
				}
				dmpDataJson.put("url", values[6]);
				dmpDataJson.put("referer", values[5]);
				dmpDataJson.put("domain", values[7]);
				dmpDataJson.put("log_source", "bu_log");
				dmpDataJson.put("ad_view", 0);
				dmpDataJson.put("vpv", 0);
				dmpDataJson.put("pa_event", "mark");
				dmpDataJson.put("trigger_type", "pv");
				dmpDataJson.put("ck", 0);
				dmpDataJson.put("pv", 1);
				// 地區資訊 [area_info_classify] null:ip不正確,N:ip比對不到
				dmpDataJson.put("ip", values[1]);
				dmpDataJson.put("area_info_source", "ip");
				// 時間資訊
				// 裝置資訊 [device_info_classify] null:user_agent為空
				dmpDataJson.put("user_agent", values[8].replaceAll("\"", ""));
				dmpDataJson.put("pa_id", values[4]);
				dmpDataJson.put("screen_x", values[9]);
				dmpDataJson.put("screen_y", values[10]);
				dmpDataJson.put("event_id", "24h");
				
				if (dmpDataJson.getAsString("referer").contains("24h.pchome.com.tw")) {
					String pageCategory = "";
					if (dmpDataJson.getAsString("referer").equals("https://24h.pchome.com.tw/")
							|| dmpDataJson.getAsString("referer").contains("htm")
							|| dmpDataJson.getAsString("referer").contains("index")
							|| dmpDataJson.getAsString("referer").contains("?fq=")
							|| dmpDataJson.getAsString("referer").contains("store/?q=")) {
						return;
					} else if (dmpDataJson.getAsString("referer").contains("?")) {
						pageCategory = dmpDataJson.getAsString("referer").substring(0,dmpDataJson.getAsString("referer").indexOf("?"));
						pageCategory = pageCategory.split("/")[pageCategory.split("/").length - 1];
					} else {
						pageCategory = dmpDataJson.getAsString("referer").split("/")[dmpDataJson.getAsString("referer").split("/").length - 1];
					}
					dmpDataJson.put("mark_value", pageCategory);
				}
				
				// 館別分類
				try {
					if (StringUtils.isNotBlank(dmpDataJson.getAsString("mark_value"))) {
						process24CategoryLevel(dmpDataJson);
					}
				} catch (Exception e) {
					System.out.println(">>>>>>>fail process 24 category level:" + e.getMessage());
					return;
				}
			} catch (Exception e) {
				System.out.println(">>>>bulog set json fail:" + e.getMessage());
				System.out.println(">>>>bulog set json fail log size:" + logStr.split(paclSymbol, -1).length);
				String[] logarray = logStr.split(paclSymbol, -1);
				for (int i = 0; i < logarray.length; i++) {
					System.out.println(">>>>bulog set json fail:[" + i + "]:" + logarray[i]);
				}
				System.out.println(">>>>bulog set json logStr:" + logStr);
				return;
			}
		}
		// 開始DMP資訊
		// 1.地區處理元件(ip 轉國家、城市)
		try {
			geoIpComponent.ipTransformGEO(dmpDataJson);
		} catch (Exception e) {
			System.out.println(">>>>process source area fail:"+dmpDataJson.getAsString("ip")+">>>>" + e.getMessage());
		}
		// 2.裝置處理元件(UserAgent轉成裝置資訊)
		try {
			deviceComponent.parseUserAgentToDevice(dmpDataJson);
		} catch (Exception e) {
			System.out.println(">>>>process source device fail:" + e.getMessage());
			System.out.println(">>>>>>logStr:" + logStr);
			System.out.println(">>>>>>fileName:" + fileName);
			return;
		}
		// 3.分類處理元件(分析click、24H、Ruten、campaign分類)
		try {
			if ((dmpDataJson.getAsString("trigger_type").equals("ck") || dmpDataJson.getAsString("log_source").equals("campaign"))) {
				// kdcl ad_click的adclass 或 campaign
				// log的adclass //&&
				// StringUtils.isNotBlank(dmpLogBeanResult.getAdClass())
				try {
					DmpLogMapper.aCategoryLogDataClick.processCategory(dmpDataJson, null);
				} catch (Exception e) {
					System.out.println(">>>>process source ck_campaign fail:" + e.getMessage());
					System.out.println(">>>>>>logStr:" + logStr);
					System.out.println(">>>>>>fileName:" + fileName);
					return;
				}
			} else if (dmpDataJson.getAsString("trigger_type").equals("pv")
					&& StringUtils.isNotBlank(dmpDataJson.getAsString("referer"))
					&& dmpDataJson.getAsString("referer").contains("ruten")) { // 露天
				try {
					DmpLogMapper.aCategoryLogDataRetun.processCategory(dmpDataJson, dBCollection_class_url);
				} catch (Exception e) {
					System.out.println(">>>>process source pv_ruten fail:" + e.getMessage());
					System.out.println(">>>>>>logStr:" + logStr);
					System.out.println(">>>>>>fileName:" + fileName);
					return;
				}
			} else if (dmpDataJson.getAsString("trigger_type").equals("pv")
					&& StringUtils.isNotBlank(dmpDataJson.getAsString("referer"))
					&& dmpDataJson.getAsString("referer").contains("24h")) { // 24h
				try {
					DmpLogMapper.aCategoryLogData24H.processCategory(dmpDataJson, dBCollection_class_url);
				} catch (Exception e) {
					System.out.println(">>>>process source pv_24h fail:" + e.getMessage());
					System.out.println(">>>>>>logStr:" + logStr);
					System.out.println(">>>>>>dmpDataJson:" + dmpDataJson);
					System.out.println(">>>>>>fileName:" + fileName);
					return;
				}
			}
		} catch (Exception e) {
			System.out.println(">>>>process source class type fail:" + e.getMessage());
			System.out.println(">>>>>>logStr:" + logStr);
			System.out.println(">>>>>>dmpDataJson:" + dmpDataJson);
			System.out.println(">>>>>>fileName:" + fileName);
			return;
		}
//		寫入reduce
		try {
			context.write(new Text(dmpDataJson.getAsString("uuid")+"<PCHOME>"+dmpDataJson.getAsString("log_source")), new Text(dmpDataJson.toString()));
		} catch (Exception e) {
			log.error(">>>>write to reduce fail:" + e.getMessage());
		}
	}

	// 處理24館別階層
	private void process24CategoryLevel(net.minidev.json.JSONObject dmpDataJson) throws Exception {
		String markValue = dmpDataJson.getAsString("mark_value");
		int level = 0;
		if (markValue.length() == 4) {
			level = 2;
		}
		if (markValue.length() >= 6) {
			level = 3;
		}
		if (categoryLevelMappingMap.containsKey(markValue)) {
			org.json.JSONObject layerJson = categoryLevelMappingMap.get(markValue);
			Iterator<String> keys = layerJson.keys();
			while (keys.hasNext()) {
				String key = keys.next();
				String value = layerJson.getString(key);
				dmpDataJson.put(key, value);
			}
		} else {
			for (String string : categoryLevelMappingList) {
				String level1 = string.split("<PCHOME>")[0];
				String level2 = string.split("<PCHOME>")[1];
				String level3 = string.split("<PCHOME>")[2];
				if (level1.equals(markValue)) {
					dmpDataJson.put("mark_layer1", "1");
					dmpDataJson.put("mark_value1", level1);
					org.json.JSONObject layerJson = new org.json.JSONObject();
					layerJson.put("mark_layer1", "1");
					layerJson.put("mark_value1", level1);
					categoryLevelMappingMap.put(markValue, layerJson);
					break;
				} else if (level2.equals(markValue)) {
					dmpDataJson.put("mark_layer1", "1");
					dmpDataJson.put("mark_value1", level1);
					dmpDataJson.put("mark_layer2", "2");
					dmpDataJson.put("mark_value2", level2);
					org.json.JSONObject layerJson = new org.json.JSONObject();
					layerJson.put("mark_layer1", "1");
					layerJson.put("mark_value1", level1);
					layerJson.put("mark_layer2", "2");
					layerJson.put("mark_value2", level2);
					categoryLevelMappingMap.put(markValue, layerJson);
					break;
				} else if (level3.equals(markValue)) {
					dmpDataJson.put("mark_layer1", "1");
					dmpDataJson.put("mark_value1", level1);
					dmpDataJson.put("mark_layer2", "2");
					dmpDataJson.put("mark_value2", level2);
					dmpDataJson.put("mark_layer3", "3");
					dmpDataJson.put("mark_value3", level3);
					org.json.JSONObject layerJson = new org.json.JSONObject();
					layerJson.put("mark_layer1", "1");
					layerJson.put("mark_value1", level1);
					layerJson.put("mark_layer2", "2");
					layerJson.put("mark_value2", level2);
					layerJson.put("mark_layer3", "3");
					layerJson.put("mark_value3", level3);
					categoryLevelMappingMap.put(markValue, layerJson);
					break;
				}
			}
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
