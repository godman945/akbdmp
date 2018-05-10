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
	
	private static int LOG_LENGTH = 30;
	private static String SYMBOL = String.valueOf(new char[] { 9, 31 });

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
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			if(StringUtils.isNotBlank(context.getConfiguration().get("spring.profiles.active"))){
				System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			}else{
				System.setProperty("spring.profiles.active", "stg");
			}
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.mongoOperations = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
			record_date = context.getConfiguration().get("job.date");
			Configuration conf = context.getConfiguration();
			
			//load 分類個資表(ClsfyGndAgeCrspTable.txt)
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
			log.info("Mapper  setup Exception: "+e.getMessage());
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		
		try {
//			//讀取kdcl、Campaign資料
			log.info("raw_data : " + value);
			
			DmpLogBean dmpDataBean = new DmpLogBean();
			String valueStr = value.toString();
			if ( valueStr.indexOf(SYMBOL) > -1 ){	//kdcl log
				//kdcl log	raw data格式
				// values[0]  date time (2018-01-04 04:57:12)
				// values[1]  memid
				// values[2]  uuid
				// values[3]  ip
				// values[4]  url
				// values[13] ck,pv
				// values[15] ad_class
				String[] values = valueStr.toString().split(SYMBOL);
				if (values.length < LOG_LENGTH) {
					log.info("values.length < " + LOG_LENGTH);
					return;
				}
				dmpDataBean.setMemid(values[1]);
				dmpDataBean.setUuid(values[2]);
				dmpDataBean.setAdClass(values[15]);
				dmpDataBean.setUrl(values[4]);
				dmpDataBean.setIp(values[3]);
				dmpDataBean.setUserAgent(values[5]);
				dmpDataBean.setDateTime(values[0]);
				dmpDataBean.setSource(values[13]);
				log.info(">>>>>> kdcl rawdata:" + valueStr);
			}else if( valueStr.indexOf(",") > -1 ){	//campaign log
				//Campaign log raw data格式
				// values[0] memid			會員帳號
				// values[1] uuid			通用唯一識別碼	
				// values[2] ad_class		分類
				// values[3] Count			數量
				// values[4] age			年齡
				// values[5] sex			性別(F|M)
				// values[6] ip_area		地區(台北市 or 空字串)
				// values[7] record_date	紀錄日期(2018-04-27)
				// values[8] Over_write		是否覆寫(true|false)
				String[] values = valueStr.toString().split(",");
				 if (values.length < 9) {
					 return;
                 }
				 dmpDataBean.setMemid(StringUtils.isBlank(values[0])? "null" :values[0]);
				 dmpDataBean.setUuid(values[1]);
				 dmpDataBean.setAdClass(values[2]);
				 dmpDataBean.setUrl("");
				 dmpDataBean.setIp(values[6]);
				 dmpDataBean.setUserAgent("");
				 dmpDataBean.setDateTime(values[7]);
				 dmpDataBean.setSource("campaign");
				 log.info(">>>>>> campaige rawdata:" + valueStr);
			}else{
				 return;
			}
			

			DmpLogBean dmpLogBeanResult = null;
			
			//地區處理元件(ip 轉國家、城市)
			dmpLogBeanResult = geoIpComponent.ipTransformGEO(dmpDataBean);
			
			//時間處理元件(日期時間字串轉成小時)			
			dmpLogBeanResult = dateTimeComponent.datetimeTransformHour(dmpLogBeanResult);
			
			//裝置處理元件
			dmpLogBeanResult = deviceComponent.parseUserAgentToDevice(dmpLogBeanResult);
			
			
			//分類處理元件(分析click、24H、Ruten、campaign分類) 
			if ( (dmpLogBeanResult.getSource().equals("ck")||dmpLogBeanResult.getSource().equals("campaign")) && StringUtils.isNotBlank(dmpLogBeanResult.getAdClass())) {	// kdcl log的ad_click 或 campaign log的adclass 
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
				dmpLogBeanResult = (DmpLogBean) aCategoryLogData.processCategory(dmpLogBeanResult, mongoOperations);
			}else if (dmpLogBeanResult.getSource().equals("pv") && StringUtils.isNotBlank(dmpLogBeanResult.getUrl()) && dmpLogBeanResult.getUrl().contains("ruten")) {	// 露天
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
				dmpLogBeanResult = (DmpLogBean) aCategoryLogData.processCategory(dmpLogBeanResult, mongoOperations);
			}else if (dmpLogBeanResult.getSource().equals("pv") && StringUtils.isNotBlank(dmpLogBeanResult.getUrl()) && dmpLogBeanResult.getUrl().contains("24h")) {		// 24h
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
				dmpLogBeanResult = (DmpLogBean) aCategoryLogData.processCategory(dmpLogBeanResult, mongoOperations);
			}else{
				return;
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
			//26.url + 27.ip + 28.record_date + 29.original_source
			
			String memid = StringUtils.isBlank(dmpLogBeanResult.getMemid()) ? "null" : dmpLogBeanResult.getMemid();
			String result = memid + SYMBOL + dmpLogBeanResult.getUuid() + SYMBOL + dmpLogBeanResult.getCategory() + SYMBOL  + dmpLogBeanResult.getCategorySource()
			+ SYMBOL + dmpLogBeanResult.getSex() + SYMBOL + dmpLogBeanResult.getSexSource() + SYMBOL + dmpLogBeanResult.getAge() + SYMBOL + dmpLogBeanResult.getAgeSource()
			+ SYMBOL + dmpLogBeanResult.getCountry() + SYMBOL + dmpLogBeanResult.getCity() + SYMBOL + dmpLogBeanResult.getAreaInfoSource()
			+ SYMBOL + dmpLogBeanResult.getDeviceInfoSource() + SYMBOL + dmpLogBeanResult.getDeviceInfo()
			+ SYMBOL + dmpLogBeanResult.getDevicePhoneInfo() + SYMBOL + dmpLogBeanResult.getDeviceOsInfo() + SYMBOL + dmpLogBeanResult.getDeviceBrowserInfo()
			+ SYMBOL + dmpLogBeanResult.getHour() + SYMBOL + dmpLogBeanResult.getTimeInfoSource() 
			+ SYMBOL +dmpLogBeanResult.getPersonalInfoApiClassify()+ SYMBOL +dmpLogBeanResult.getPersonalInfoClassify()
			+ SYMBOL +dmpLogBeanResult.getClassAdClickClassify() + SYMBOL +dmpLogBeanResult.getClass24hUrlClassify()+ SYMBOL +dmpLogBeanResult.getClassRutenUrlClassify()
			+ SYMBOL +dmpLogBeanResult.getAreaInfoClassify()+ SYMBOL +dmpLogBeanResult.getDeviceInfoClassify()+ SYMBOL +dmpLogBeanResult.getTimeInfoClassify()
			+ SYMBOL + dmpLogBeanResult.getUrl() + SYMBOL + dmpLogBeanResult.getIp() + SYMBOL + dmpLogBeanResult.getRecordDate()+ SYMBOL + dmpLogBeanResult.getSource();
			
			log.info(">>>>>> Mapper write key:" + result);
			
			keyOut.set(result);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error(">>>>>> " + e.getMessage());
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

}
