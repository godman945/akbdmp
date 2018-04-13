package com.pchome.hadoopdmp.mongo.job;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

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

import com.pchome.hadoopdmp.enumerate.CategoryLogEnum;
import com.pchome.hadoopdmp.mapreduce.job.factory.ACategoryLogData;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogBean;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogFactory;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;

@Component
public class MongoDbMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog(this.getClass());
	
	private static int LOG_LENGTH = 30;
	private static String SYMBOL = String.valueOf(new char[] { 9, 31 });

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();//分類表	
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();	 //分類個資表
	private MongoOperations mongoOperations;

	private int adClick_process = 0;
	private int tweenFour_process = 0;
	private int ruten_process = 0;
	private long time1, time2,time3;
	@Override
	public void setup(Context context) {
		log.info(">>>>>> mongoDb Mapper  setup >>>>>>>>>>>>>>>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
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
//			//load 分類個資表(ClsfyGndAgeCrspTable.txt)
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
//
//			}
//			
//		} catch (Exception e) {
//			log.info("Mapper  setup Exception: "+e.getMessage());
//		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
//		// values[1] //memid
//		// values[2] //uuid
//		// values[4] //url
//		// values[13] //ck,pv
//		// values[15] //ad_class
		try {
//			String[] values = value.toString().split(SYMBOL);
//			if (values.length < LOG_LENGTH) {
//				log.info("values.length < " + LOG_LENGTH);
//				return;
//			}
//			
////			log.info("raw_data [memid] : " + values[1]);
////			log.info("raw_data [uuid] : " + values[2]);
////			log.info("raw_data [url] : " + values[4]);
////			log.info("raw_data [ck,pv] : " + values[13]);
////			log.info("raw_data [ad_class] : " + values[15]);
//
//			CategoryLogBean categoryLogBean = new CategoryLogBean();
//			CategoryLogBean categoryLogBeanResult = null;
//			// ad_click
//			if (values[13].equals("ck") && StringUtils.isNotBlank(values[4]) && StringUtils.isNotBlank(values[15])) {
//				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
//				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(values, categoryLogBean, mongoOperations);
//				
//			}
//			else
////			
//			// 露天
//			if (values[13].equals("pv") && StringUtils.isNotBlank(values[4]) && values[4].contains("ruten")) {
//				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
//				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(values, categoryLogBean, mongoOperations);
//			}
//				else
////
////			// 24h
//			if (values[13].equals("pv") && StringUtils.isNotBlank(values[4]) && values[4].contains("24h")) {
//				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
//				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(values, categoryLogBean, mongoOperations);
//			}
//			else{
//				return;
//			}
//
//			if (categoryLogBeanResult == null) {
//				return;
//			}
//			
//			categoryLogBeanResult.setRecodeDate(record_date);
//			// 0:Memid + 1:Uuid + 2:AdClass + 3:Age + 4:Sex + 5:Source + 6:RecodeDate + 7:Type + 8:Classify
//			String memid = StringUtils.isBlank(categoryLogBeanResult.getMemid()) ? "null" : categoryLogBeanResult.getMemid();
//			String result = memid + SYMBOL + categoryLogBeanResult.getUuid() + SYMBOL + categoryLogBeanResult.getAdClass() + SYMBOL + categoryLogBeanResult.getAge() + SYMBOL + categoryLogBeanResult.getSex() + SYMBOL + categoryLogBeanResult.getSource()+ SYMBOL +categoryLogBeanResult.getRecodeDate() + SYMBOL + categoryLogBeanResult.getType() + SYMBOL + values[4] + SYMBOL + categoryLogBeanResult.getType()+"_"+categoryLogBeanResult.getSource()+"_"+categoryLogBeanResult.getBehaviorClassify() + SYMBOL + "user_info_Classify_"+categoryLogBeanResult.getPersonalInfoClassify();
			log.info(">>>>>> Mapper write key:" + value);
			keyOut.set(value);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error(">>>>>> " + e.getMessage());
		}

	}

//	 @Autowired
//	 MongoOperations mongoOperations;
	 public void test() throws Exception{
//	 System.out.println("BBB");
//	 Query query = new
//	 Query(Criteria.where("_id").is("59404c00e4b0ed734829caf4"));
//	 ClassUrlMongoBean classUrlMongoBean = (ClassUrlMongoBean)
//	 mongoOperations.findOne(query, ClassUrlMongoBean.class);
//	 System.out.println(classUrlMongoBean.getUrl());
//	 System.out.println("AAA");
	 }

//	public static void main(String[] args) throws Exception {
//		 CategoryLogMapper categoryLogMapper = new CategoryLogMapper();
//		 categoryLogMapper.map(null, null, null);
//
////		System.setProperty("spring.profiles.active", "stg");
////		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
////		CategoryLogMapper categoryLogMapper = ctx.getBean(CategoryLogMapper.class);
////		categoryLogMapper.test();
//
//	}

	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
}
