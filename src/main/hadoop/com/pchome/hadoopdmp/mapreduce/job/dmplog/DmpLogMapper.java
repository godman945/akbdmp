package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.File;
import java.net.InetAddress;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import com.maxmind.geoip2.DatabaseReader;
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
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;

@Component
public class DmpLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("DmpLogMapper");
	
	private static int recordCount = 0;
	private static int kdclLogLength = 30;
	private static int campaignLogLength = 9;
	private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
	private static String campaignSymbol = ",";

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();	 //Ruten分類表
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
	public static DateTimeComponent dateTimeComponent = new DateTimeComponent();
	public static DeviceComponent deviceComponent = new DeviceComponent();
	private MongoOperations mongoOperations;
	public static DatabaseReader reader = null;
	public static InetAddress ipAddress = null;

	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.mongoOperations = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
			record_date = context.getConfiguration().get("job.date");
			Configuration conf = context.getConfiguration();
			
			//load 推估分類個資表(ClsfyGndAgeCrspTable.txt)
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

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		
		try {
			//讀取kdcl、Campaign資料
			log.info("raw_data : " + value);
			
			DmpLogBean dmpDataBean = new DmpLogBean();
			String valueStr = value.toString();
			
			if ( valueStr.indexOf(kdclSymbol) > -1 ){	//kdcl log	raw data格式
				// values[0]  date time (2018-01-04 04:57:12)
				// values[1]  memid
				// values[2]  uuid
				// values[3]  ip
				// values[4]  url
				// values[5]  UserAgent
				// values[13] ck,pv
				// values[15] ad_class
				String[] values = valueStr.toString().split(kdclSymbol);
				if (values.length < kdclLogLength) {
					log.info("values.length < " + kdclLogLength);
					return;
				}
				dmpDataBean.setDateTime(values[0]);
				dmpDataBean.setMemid(values[1]);
				dmpDataBean.setUuid(values[2]);
				dmpDataBean.setIp(values[3]);
				dmpDataBean.setUrl(values[4]);
				dmpDataBean.setUserAgent(values[5]);
				dmpDataBean.setSource(values[13]);
				dmpDataBean.setAdClass(values[15]);
				dmpDataBean.setAge("null");
				dmpDataBean.setSex("null");
				log.info(">>>>>> kdcl rawdata:" + valueStr);
			}else if( valueStr.indexOf(campaignSymbol) > -1 ){	//Campaign log raw data格式
				// values[0] memid			會員帳號
				// values[1] uuid			通用唯一識別碼	
				// values[2] ad_class		分類
				// values[3] Count			數量
				// values[4] age			年齡 (0或空字串)
				// values[5] sex			性別(F|M)
				// values[6] ip_area		地區(台北市 or 空字串)
				// values[7] record_date	紀錄日期(2018-04-27)
				// values[8] Over_write		是否覆寫(true|false)
				String[] values = valueStr.toString().split(campaignSymbol);
				 if (values.length < campaignLogLength) {
					 log.info("values.length < " + campaignLogLength);
					 return;
                 }
				 
				 dmpDataBean.setDateTime(values[7]);
				 dmpDataBean.setMemid(StringUtils.isBlank(values[0])? "null" :values[0]);
				 dmpDataBean.setUuid(values[1]);
				 dmpDataBean.setIp(values[6]);
				 dmpDataBean.setUrl("");
				 dmpDataBean.setUserAgent("");
				 dmpDataBean.setSource("campaign");
				 dmpDataBean.setAdClass(values[2]);
				 
				 if (StringUtils.equals(values[4], "0")){
					 dmpDataBean.setAge("null");
				 }else{
					 dmpDataBean.setAge(values[4]);
				 }
				 
				 if (StringUtils.isBlank(values[5])){
					 dmpDataBean.setSex("null");
				 }else{
					 dmpDataBean.setSex(values[5]);
				 }
				 log.info(">>>>>> campaige rawdata:" + valueStr);
			}else{
				 return;
			}
			

			DmpLogBean dmpLogBeanResult = new DmpLogBean();
			
			//地區處理元件(ip 轉國家、城市)
			dmpLogBeanResult = geoIpComponent.ipTransformGEO(dmpDataBean);
			
			//時間處理元件(日期時間字串轉成小時)			
			dmpLogBeanResult = dateTimeComponent.datetimeTransformHour(dmpLogBeanResult); 
			
			//裝置處理元件(UserAgent轉成裝置資訊)
			dmpLogBeanResult = deviceComponent.parseUserAgentToDevice(dmpLogBeanResult);
			
			
			//分類處理元件(分析click、24H、Ruten、campaign分類) 
			if ( (dmpLogBeanResult.getSource().equals("ck")||dmpLogBeanResult.getSource().equals("campaign")) ) {	// kdcl ad_click的adclass  或   campaign log的adclass 	//&& StringUtils.isNotBlank(dmpLogBeanResult.getAdClass())
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
				dmpLogBeanResult = (DmpLogBean) aCategoryLogData.processCategory(dmpLogBeanResult, mongoOperations);
			}else if (dmpLogBeanResult.getSource().equals("pv") && StringUtils.isNotBlank(dmpLogBeanResult.getUrl()) && dmpLogBeanResult.getUrl().contains("ruten")) {	// 露天
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
				dmpLogBeanResult = (DmpLogBean) aCategoryLogData.processCategory(dmpLogBeanResult, mongoOperations);
			}else if (dmpLogBeanResult.getSource().equals("pv") && StringUtils.isNotBlank(dmpLogBeanResult.getUrl()) && dmpLogBeanResult.getUrl().contains("24h")) {		// 24h
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
				dmpLogBeanResult = (DmpLogBean) aCategoryLogData.processCategory(dmpLogBeanResult, mongoOperations);
			}else if ( dmpLogBeanResult.getSource().equals("pv") ){
				dmpDataBean.setSource("kdcl");
			}
			
			//個資處理元件
			dmpLogBeanResult = personalInfoComponent.processPersonalInfo(dmpLogBeanResult, mongoOperations);
			
			//紀錄日期
			dmpLogBeanResult.setRecordDate(record_date);
			
			
			// 0:memid + 1:uuid + 2:category + 3.categorySource 
			// 4.sex + 5.sexSource + 6.age + 7.ageSource 
			// 8.country + 9.city + 10.areaInfoSource
			//11.device_info_source + 12.device_info 
			//13.device_phone_info + 14.device_os_info + 15.device_browser_info 
			//16.time_info_hour + 17.time_info_source 
			
			//classify
			//18.personal_info_api + 19.personal_info 
			//20.class_ad_click + 21.class_24h_url + 22.class_ruten_url
			//23.area_info + 24.device_info + 25.time_info
			//26.url + 27.ip + 28.record_date + 29.org_source(kdcl、campaign) 
			//30.date_time + 31.user_agent +32.ad_class + 33.record_count
			
			recordCount = recordCount + 1;
			String memid = StringUtils.isBlank(dmpLogBeanResult.getMemid()) ? "null" : dmpLogBeanResult.getMemid();
			String result = memid + kdclSymbol + dmpLogBeanResult.getUuid() + kdclSymbol + dmpLogBeanResult.getCategory() + kdclSymbol  + dmpLogBeanResult.getCategorySource()
			+ kdclSymbol + dmpLogBeanResult.getSex() + kdclSymbol + dmpLogBeanResult.getSexSource() + kdclSymbol + dmpLogBeanResult.getAge() + kdclSymbol + dmpLogBeanResult.getAgeSource()
			+ kdclSymbol + dmpLogBeanResult.getCountry() + kdclSymbol + dmpLogBeanResult.getCity() + kdclSymbol + dmpLogBeanResult.getAreaInfoSource()
			+ kdclSymbol + dmpLogBeanResult.getDeviceInfoSource() + kdclSymbol + dmpLogBeanResult.getDeviceInfo()
			+ kdclSymbol + dmpLogBeanResult.getDevicePhoneInfo() + kdclSymbol + dmpLogBeanResult.getDeviceOsInfo() + kdclSymbol + dmpLogBeanResult.getDeviceBrowserInfo()
			+ kdclSymbol + dmpLogBeanResult.getHour() + kdclSymbol + dmpLogBeanResult.getTimeInfoSource() 
			+ kdclSymbol +dmpLogBeanResult.getPersonalInfoApiClassify()+ kdclSymbol +dmpLogBeanResult.getPersonalInfoClassify()
			+ kdclSymbol +dmpLogBeanResult.getClassAdClickClassify() + kdclSymbol +dmpLogBeanResult.getClass24hUrlClassify()+ kdclSymbol +dmpLogBeanResult.getClassRutenUrlClassify()
			+ kdclSymbol +dmpLogBeanResult.getAreaInfoClassify()+ kdclSymbol +dmpLogBeanResult.getDeviceInfoClassify()+ kdclSymbol +dmpLogBeanResult.getTimeInfoClassify()
			+ kdclSymbol + dmpLogBeanResult.getUrl() + kdclSymbol + dmpLogBeanResult.getIp() + kdclSymbol + dmpLogBeanResult.getRecordDate()+ kdclSymbol + dmpLogBeanResult.getSource()
			+ kdclSymbol + dmpLogBeanResult.getDateTime() + kdclSymbol + dmpLogBeanResult.getUserAgent() + kdclSymbol + dmpLogBeanResult.getAdClass() 
			+ kdclSymbol + recordCount;
			
			log.info(">>>>>> Mapper write key:" + result);
			
			keyOut.set(result);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error("Mapper error>>>>>> " +e);
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
	

//	 @Autowired
//	 MongoOperations mongoOperations;
//	 public void test() throws Exception{
//	 System.out.println("BBB");
//	 Query query = new
//	 Query(Criteria.where("_id").is("59404c00e4b0ed734829caf4"));
//	 ClassUrlMongoBean classUrlMongoBean = (ClassUrlMongoBean)
//	 mongoOperations.findOne(query, ClassUrlMongoBean.class);
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
