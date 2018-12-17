//package com.pchome.hadoopdmp.mapreduce.job.Geoip2;
//import java.io.File;
//import java.net.InetAddress;
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.AnnotationConfigApplicationContext;
//import org.springframework.data.mongodb.core.MongoOperations;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.stereotype.Component;
//
//import com.maxmind.geoip2.DatabaseReader;
//import com.maxmind.geoip2.model.CityResponse;
//import com.maxmind.geoip2.record.City;
//import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
//import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryCodeBean;
//import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
//import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;
//
//@Component
//public class Geoip2LogMapper extends Mapper<LongWritable, Text, Text, Text> {
//	Log log = LogFactory.getLog("Geoip2LogMapper");
//	
//	private static int LOG_LENGTH = 30;
//	private static String SYMBOL = String.valueOf(new char[] { 9, 31 });
//
//	private Text keyOut = new Text();
//	private Text valueOut = new Text();
//
//	public static String record_date;
////	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();//分類表	
////	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();	 //分類個資表
////	public static List<CategoryCodeBean> categoryBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
//	private MongoOperations mongoOperations;
//	
//	DatabaseReader reader = null;
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
//			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
//			
//			//IP轉城市
////			File database = new File("D:/PCWork0301/Bessie/Work/新DMP系統/GeoLite2-City.mmdb");
//			File database = new File(path[0].toString());
//			reader = new DatabaseReader.Builder(database).build(); 
//			
//		} catch (Exception e) {
//			log.info("Mapper  setup Exception: "+e.getMessage());
//		}
//	}
//
//	@Override
//	public void map(LongWritable offset, Text value, Context context) {
//		try {
//			
//			String valuestr = value.toString();
//			
//			String[] values = valuestr.split(SYMBOL);
//			
//			if (values.length < LOG_LENGTH) {
//				log.info("values.length < " + LOG_LENGTH);
//				return;
//			}
//			
//			String ip = values[3];
//			
//			//IP轉城市
//			InetAddress ipAddress = InetAddress.getByName(ip);//台北市
//			CityResponse response = reader.city(ipAddress);   
//			City city = response.getCity();
//			String cityStr = city.getNames().get("zh-CN");
//			
//			String result = ip + SYMBOL + cityStr +"   >>>>>ip log>>>>> ";
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
