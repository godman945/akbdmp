//package com.pchome.hadoopdmp.mapreduce.job.RawData.TEST;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.AnnotationConfigApplicationContext;
//import org.springframework.data.mongodb.core.MongoOperations;
//import org.springframework.stereotype.Component;
//
//import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;
//import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
//import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;
//
//@Component
//public class RawDataLogMapper extends Mapper<LongWritable, Text, Text, Text> {
//	Log log = LogFactory.getLog("CategoryLogMapper");
//	
//	private static int recordCount = 0;
//	private static int kdclLogLength = 30;
//	private static int campaignLogLength = 9;
//	private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
//	private static String campaignSymbol = ",";
//
//	private Text keyOut = new Text();
//	private Text valueOut = new Text();
//
//	public static String record_date;
//	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();//分類表	
//	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();	 //分類個資表
//	private MongoOperations mongoOperations;
//
//	private int adClick_process = 0;
//	private int tweenFour_process = 0;
//	private int ruten_process = 0;
//	private long time1, time2,time3;
//	@Override
//	public void setup(Context context) {
//		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
//		time1 = System.currentTimeMillis();
//		try {
//			if(StringUtils.isNotBlank(context.getConfiguration().get("spring.profiles.active"))){
//				System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
//			}else{
//				System.setProperty("spring.profiles.active", "stg");
//			}
//			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//			this.mongoOperations = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
//			record_date = context.getConfiguration().get("job.date");
//			Configuration conf = context.getConfiguration();
//			
////			//load 分類個資表(ClsfyGndAgeCrspTable.txt)
////			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
////			Path clsfyTable = Paths.get(path[1].toString());
////			Charset charset = Charset.forName("UTF-8");
////			List<String> lines = Files.readAllLines(clsfyTable, charset);
////			for (String line : lines) {
////				String[] tmpStrAry = line.split(";"); // 0001000000000000;M,35
////				String[] tmpStrAry2 = tmpStrAry[1].split(",");
////				clsfyCraspMap.put(tmpStrAry[0],new combinedValue(tmpStrAry[1].split(",")[0], tmpStrAry2.length > 1 ? tmpStrAry2[1] : ""));
////			}
////			
//
////			// load 分類表(pfp_ad_category_new.csv)
////			Path cate_path = Paths.get(path[0].toString());
////			charset = Charset.forName("UTF-8");
////			int maxCateLvl = 4;
////			categoryList = new ArrayList<Map<String, String>>();
////			for (int i = 0; i < maxCateLvl; i++) {
////				categoryList.add(new HashMap<String, String>());
////			}
////			lines.clear();
////			lines = Files.readAllLines(cate_path, charset);
////			// 將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
////			for (String line : lines) {
////				String[] tmpStr = line.split(";");
////				int lvl = Integer.parseInt(tmpStr[5].replaceAll("\"", "").trim());
////				if (lvl <= maxCateLvl) {
////					categoryList.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
////				}
////
////			}
//			
//		} catch (Exception e) {
//			log.info("Mapper  setup Exception: "+e.getMessage());
//		}
//	}
//
//	@Override
//	public void map(LongWritable offset, Text value, Context context) {
//		// values[1] //memid
//		// values[2] //uuid
//		// values[4] //url
//		// values[13] //ck,pv
//		// values[15] //ad_class
//		try {
//			//讀取kdcl、Campaign資料
//			log.info("raw_data : " + value);
//			
//			DmpLogBean dmpDataBean = new DmpLogBean();
//			String valueStr = value.toString();
//			
//			if ( valueStr.indexOf(kdclSymbol) > -1 ){	//kdcl log	raw data格式
//				// values[0]  date time (2018-01-04 04:57:12)
//				// values[1]  memid
//				// values[2]  uuid
//				// values[3]  ip
//				// values[4]  url
//				// values[5]  UserAgent
//				// values[13] ck,pv
//				// values[15] ad_class
//				String[] values = valueStr.toString().split(kdclSymbol);
//				if (values.length < kdclLogLength) {
//					log.info("values.length < " + kdclLogLength);
//					return;
//				}
//				dmpDataBean.setDateTime(values[0]);
//				dmpDataBean.setMemid(values[1]);
//				dmpDataBean.setUuid(values[2]);
//				dmpDataBean.setIp(values[3]);
//				dmpDataBean.setUrl(values[4]);
//				dmpDataBean.setUserAgent(values[5]);
//				dmpDataBean.setSource(values[13]);
//				dmpDataBean.setAdClass(values[15]);
//				dmpDataBean.setAge("null");
//				dmpDataBean.setSex("null");
//				log.info(">>>>>> kdcl rawdata:" + valueStr);
//			}else if( valueStr.indexOf(campaignSymbol) > -1 ){	//Campaign log raw data格式
//				// values[0] memid			會員帳號
//				// values[1] uuid			通用唯一識別碼	
//				// values[2] ad_class		分類
//				// values[3] Count			數量
//				// values[4] age			年齡 (0或空字串)
//				// values[5] sex			性別(F|M)
//				// values[6] ip_area		地區(台北市 or 空字串)
//				// values[7] record_date	紀錄日期(2018-04-27)
//				// values[8] Over_write		是否覆寫(true|false)
//				String[] values = valueStr.toString().split(campaignSymbol);
//				 if (values.length < campaignLogLength) {
//					 log.info("values.length < " + campaignLogLength);
//					 return;
//                 }
//				 
//				 dmpDataBean.setDateTime(values[7]);
//				 dmpDataBean.setMemid(StringUtils.isBlank(values[0])? "null" :values[0]);
//				 dmpDataBean.setUuid(values[1]);
//				 dmpDataBean.setIp(values[6]);
//				 dmpDataBean.setUrl("");
//				 dmpDataBean.setUserAgent("");
//				 dmpDataBean.setSource("campaign");
//				 dmpDataBean.setAdClass(values[2]);
//				 
//				 if (StringUtils.equals(values[4], "0")){
//					 dmpDataBean.setAge("null");
//				 }else{
//					 dmpDataBean.setAge(values[4]);
//				 }
//				 
//				 if (StringUtils.isBlank(values[5])){
//					 dmpDataBean.setSex("null");
//				 }else{
//					 dmpDataBean.setSex(values[5]);
//				 }
//				 log.info(">>>>>> campaige rawdata:" + valueStr);
//			}else{
//				 return;
//			}
//			
//			
//			
//			
//			
//			
//			
//			String result = valueStr ;
//			
//			
//			log.info(">>>>>> Mapper write key:" + result);
//			keyOut.set(result);
//			context.write(keyOut, valueOut);
//			
//		} catch (Exception e) {
//			log.error(">>>>>> " + e.getMessage());
//		}
//
//	}
//
////	 @Autowired
////	 MongoOperations mongoOperations;
//	 public void test() throws Exception{
////	 System.out.println("BBB");
////	 Query query = new
////	 Query(Criteria.where("_id").is("59404c00e4b0ed734829caf4"));
////	 ClassUrlMongoBean classUrlMongoBean = (ClassUrlMongoBean)
////	 mongoOperations.findOne(query, ClassUrlMongoBean.class);
////	 System.out.println(classUrlMongoBean.getUrl());
////	 System.out.println("AAA");
//	 }
//
////	public static void main(String[] args) throws Exception {
////		 CategoryLogMapper categoryLogMapper = new CategoryLogMapper();
////		 categoryLogMapper.map(null, null, null);
////
//////		System.setProperty("spring.profiles.active", "stg");
//////		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//////		CategoryLogMapper categoryLogMapper = ctx.getBean(CategoryLogMapper.class);
//////		categoryLogMapper.test();
////
////	}
//
//	public class combinedValue {
//		public String gender;
//		public String age;
//
//		public combinedValue(String gender, String age) {
//			this.gender = gender;
//			this.age = age;
//		}
//	}
//}
