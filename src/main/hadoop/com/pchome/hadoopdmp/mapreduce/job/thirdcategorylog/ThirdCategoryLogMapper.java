package com.pchome.hadoopdmp.mapreduce.job.thirdcategorylog;

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
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.maxmind.geoip2.DatabaseReader;
import com.mongodb.DB;
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

@Component
public class ThirdCategoryLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("ThirdCategoryLogMapper");
	
//	private static int recordCount = 0;
//	private static int kdclLogLength = 30;
//	private static int campaignLogLength = 9;
//	private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
//	private static String campaignSymbol = ",";

	private Text keyOut = new Text();
	private Text valueOut = new Text();

//	public static String record_date;
//	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
//	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
//	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
//	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();	 //Ruten分類表
//	public static ArrayList<String> prodFileList = new ArrayList<String>();	 								     //24h、ruten第3分類對照表
//	public static ThirdAdClassComponent thirdAdClassComponent = new ThirdAdClassComponent();
//	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
//	public static GeoIpComponent geoIpComponent = new GeoIpComponent();
//	public static DateTimeComponent dateTimeComponent = new DateTimeComponent();
//	public static DeviceComponent deviceComponent = new DeviceComponent();
//	private DB mongoOrgOperations;
//	public static DatabaseReader reader = null;
//	public static InetAddress ipAddress = null;

	@Override
	public void setup(Context context) {
		log.info(">>>>>> Third Category Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
//		try {
//			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
//			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
//			record_date = context.getConfiguration().get("job.date");
//			Configuration conf = context.getConfiguration();
//			
//			//load 推估分類個資表(ClsfyGndAgeCrspTable.txt)
//			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
//			Path clsfyTable = Paths.get(path[1].toString());
//			Charset charset = Charset.forName("UTF-8");
//			List<String> lines = Files.readAllLines(clsfyTable, charset);
//			for (String line : lines) {
//				String[] tmpStrAry = line.split(";"); // 0001000000000000;M,35
//				String[] tmpStrAry2 = tmpStrAry[1].split(",");
//				clsfyCraspMap.put(tmpStrAry[0],new combinedValue(tmpStrAry[1].split(",")[0], tmpStrAry2.length > 1 ? tmpStrAry2[1] : ""));
//			}
//			
//
//			// load 分類表(pfp_ad_category_new.csv)
//			Path cate_path = Paths.get(path[0].toString());
//			charset = Charset.forName("UTF-8");
//			int maxCateLvl = 4;
//			categoryList = new ArrayList<Map<String, String>>();
//			for (int i = 0; i < maxCateLvl; i++) {
//				categoryList.add(new HashMap<String, String>());
//			}
//			lines.clear();
//			lines = Files.readAllLines(cate_path, charset);
//			// 將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
//			for (String line : lines) {
//				String[] tmpStr = line.split(";");
//				int lvl = Integer.parseInt(tmpStr[5].replaceAll("\"", "").trim());
//				if (lvl <= maxCateLvl) {
//					categoryList.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
//				}
//			}
//			
//			// load 24h分類表(DMP_24h_category.csv)
//			Path category24HPath = Paths.get(path[3].toString());
//			List<String> lines24H = Files.readAllLines(category24HPath, charset);
//			for (String line : lines24H) {
//				CategoryCodeBean categoryBean = new CategoryCodeBean();
//				
//				String[] tmpStrAry = line.split(","); // 0001000000000000;M,35
//				
//				categoryBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
//				categoryBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
//				categoryBean.setBreadCrumb(tmpStrAry[2].replaceAll("\"", ""));
//				categoryBean.setEnglishCode(tmpStrAry[3].replaceAll("\"", ""));
//				
//				category24hBeanList.add(categoryBean);
//			}
//			
//			// load Ruten分類表(DMP_Ruten_category.csv)
//			Path categoryRutenPath = Paths.get(path[4].toString());
//			List<String> linesRuten = Files.readAllLines(categoryRutenPath, charset);
//			for (String line : linesRuten) {
//				CategoryRutenCodeBean categoryRutenBean = new CategoryRutenCodeBean();
//				
//				String[] tmpStrAry = line.split(","); //"0001000000000000","電腦、電腦周邊"
//				
//				categoryRutenBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
//				categoryRutenBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
//				
//				categoryRutenBeanList.add(categoryRutenBean);
//			}
//			
//			//IP轉城市
//			File database = new File(path[5].toString());
//			reader = new DatabaseReader.Builder(database).build();  
//			
//			
//			//load 24h、ruten第3分類對照表(ThirdAdClassTable.txt)
//			Path thirdAdClassPath = Paths.get(path[6].toString());
//			charset = Charset.forName("UTF-8");
//			List<String> thirdAdClassLines = Files.readAllLines(thirdAdClassPath, charset);
//			for (String line : thirdAdClassLines) {
//				prodFileList.add(line);
//			}
//			
//		} catch (Exception e) {
//			log.error("Mapper setup error>>>>>> " +e);
//		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		try {
			//讀取kdcl、Campaign資料
//			log.info("raw_data : " + value);
			
//			DmpLogBean dmpDataBean =  new DmpLogBean();
			String valueStr = value.toString();
			
			log.info(">>>>>>ThirdCategoryLogMapper Mapper write key:" + valueStr);
			
			keyOut.set(valueStr);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error("Mapper error>>>>>> " +e); 
		}
	}
	
//	public DmpLogBean dmpBeanIntegrate(DmpLogBean dmpLogBeanResult) throws Exception {
//		dmpLogBeanResult.setMemid( StringUtils.isBlank(dmpLogBeanResult.getMemid()) ? "null" : dmpLogBeanResult.getMemid());
//		dmpLogBeanResult.setUuid( StringUtils.isBlank(dmpLogBeanResult.getUuid()) ? "null" : dmpLogBeanResult.getUuid());
//		dmpLogBeanResult.setCategory( StringUtils.isBlank(dmpLogBeanResult.getCategory()) ? "null" : dmpLogBeanResult.getCategory());
//		dmpLogBeanResult.setCategorySource( StringUtils.isBlank(dmpLogBeanResult.getCategorySource()) ? "null" : dmpLogBeanResult.getCategorySource());
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
//		//prod_class_info
//				JSONObject prodClassInfoJson = new JSONObject();
//				prodClassInfoJson.put("value", dmpLogBeanResult.getProdClassInfo());
//				prodClassInfoJson.put("source","null");
//		
//		//dataJson
//		JSONObject dataJson = new JSONObject();
//		dataJson.put("category_info", categoryInfoJson);
//		dataJson.put("prod_class_info", prodClassInfoJson);
//		
//		//send Kafka Json
//		JSONObject sendKafkaJson = new JSONObject();
//		sendKafkaJson.put("key", keyJson);
//		sendKafkaJson.put("data", dataJson);
//		sendKafkaJson.put("url",  dmpLogBeanResult.getUrl());
//		sendKafkaJson.put("record_date", dmpLogBeanResult.getRecordDate());
//		sendKafkaJson.put("org_source", dmpLogBeanResult.getSource());	//(kdcl、campaign)
//		sendKafkaJson.put("ad_class", dmpLogBeanResult.getAdClass());
//		sendKafkaJson.put("md5Url", dmpLogBeanResult.getUrlToMd5());
//		sendKafkaJson.put("record_count", recordCount);
//		
//		return sendKafkaJson.toString();
//	}
	
	
//	public class combinedValue {
//		public String gender;
//		public String age;
//
//		public combinedValue(String gender, String age) {
//			this.gender = gender;
//			this.age = age;
//		}
//	}
	

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
