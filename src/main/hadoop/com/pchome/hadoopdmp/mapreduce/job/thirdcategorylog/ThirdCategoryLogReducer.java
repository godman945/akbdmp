package com.pchome.hadoopdmp.mapreduce.job.thirdcategorylog;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.pchome.hadoopdmp.dao.PfpAdCategoryNewDAO;
import com.pchome.hadoopdmp.dao.SequenceDAO;
import com.pchome.hadoopdmp.mapreduce.job.component.HttpUtil;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodborg.MongodbOrgHadoopConfig;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class ThirdCategoryLogReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("ThirdCategoryLogReducer");
	
	private Text keyOut = new Text();
	
	private Text valueOut = new Text();

	public static String record_date;

	private String kafkaMetadataBrokerlist;

	private String kafkaAcks;

	private String kafkaRetries;

	private String kafkaBatchSize;

	private String kafkaLingerMs;

	private String kafkaBufferMemory;

	private String kafkaSerializerClass;

	private String kafkaKeySerializer;

	private String kafkaValueSerializer;

	public static Producer<String, String> producer = null;

	public RedisTemplate<String, Object> redisTemplate = null;
	
	public int count;
	
	public JSONParser jsonParser = null;
	
	private DB mongoOrgOperations;
	
	private DBCollection dBCollection;
	
	private int crawlerCount = 0; 
	
	private PfpAdCategoryNewDAO categoryDAO = null;
	
	private SequenceDAO sequenceDAO = null;
	
	public static ArrayList<String> prodFileList = new ArrayList<String>();	 								     //24h、ruten第3分類對照表
	
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		log.info(">>>>>>Third Category  Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.mongoOrgOperations = ctx.getBean(MongodbOrgHadoopConfig.class).mongoProducer();
			this.redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
			this.kafkaMetadataBrokerlist = ctx.getEnvironment().getProperty("kafka.metadata.broker.list");
			this.kafkaAcks = ctx.getEnvironment().getProperty("kafka.acks");
			this.kafkaRetries = ctx.getEnvironment().getProperty("kafka.retries");
			this.kafkaBatchSize = ctx.getEnvironment().getProperty("kafka.batch.size");
			this.kafkaLingerMs = ctx.getEnvironment().getProperty("kafka.linger.ms");
			this.kafkaBufferMemory = ctx.getEnvironment().getProperty("kafka.buffer.memory");
			this.kafkaSerializerClass = ctx.getEnvironment().getProperty("kafka.serializer.class");
			this.kafkaKeySerializer = ctx.getEnvironment().getProperty("kafka.key.serializer");
			this.kafkaValueSerializer = ctx.getEnvironment().getProperty("kafka.value.serializer");
			
			Properties props = new Properties();
			props.put("bootstrap.servers", kafkaMetadataBrokerlist);
			props.put("acks", kafkaAcks);
			props.put("retries", kafkaRetries);
			props.put("batch.size", kafkaBatchSize);
			props.put("linger.ms", kafkaLingerMs);
			props.put("buffer.memory", kafkaBufferMemory);
			props.put("serializer.class", kafkaSerializerClass);
			props.put("key.serializer", kafkaKeySerializer);
			props.put("value.serializer", kafkaValueSerializer);
			producer = new KafkaProducer<String, String>(props);
			jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
		
			String recordDate = context.getConfiguration().get("job.date");
			String env = context.getConfiguration().get("spring.profiles.active");
			
			//mysql init
			this.categoryDAO = new PfpAdCategoryNewDAO();
			this.categoryDAO.dbInit();
			this.sequenceDAO = new SequenceDAO();
			this.sequenceDAO.dbInit();
			
			
			//load 24h、ruten第3分類對照表(ThirdAdClassTable.txt)
			Configuration conf = context.getConfiguration();
			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
			Charset charset = Charset.forName("UTF-8");
			Path thirdAdClassPath = Paths.get(path[0].toString());
			charset = Charset.forName("UTF-8");
			List<String> thirdAdClassLines = Files.readAllLines(thirdAdClassPath, charset);
			for (String line : thirdAdClassLines) {
				prodFileList.add(line);
			}
			
			
			
			
		} catch (Throwable e) {
			log.error("Third Category reduce setup error>>>>>> " +e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		
		log.info(">>>>>>ThirdCategoryLogReducer reduce start : " + mapperKey.toString());
		
		log.info(">>>Reducer prodFileList.size() : "+prodFileList.size());
		
		
//		
//		try {
//			log.info(">>>>>>ThirdCategoryLogReducer reduce start : " + mapperKey.toString());
//			
//			JSONObject jsonObjOrg = (net.minidev.json.JSONObject)jsonParser.parse(mapperKey.toString());
//	//		log.info("str: "+jsonObjOrg);
//			
//			JSONObject dataObj =  (JSONObject) jsonObjOrg.get("data");
//			JSONArray categoryArray =  (JSONArray) dataObj.get("category_info");
//	//		log.info("category_info: "+categoryArray);
//			
//			
//			
//			JSONArray prodClassinfoAry = new JSONArray();
//			for (Object object : categoryArray) {
//				ArrayList<String> thirdCategoryList = new ArrayList<String>();  //放第3分類list
//				JSONObject infoJson = (JSONObject) object;
//				String categorySource = infoJson.getAsString("source");
//				String categoryValue = infoJson.getAsString("value");
//				String categoryUrl = infoJson.getAsString("url");
//				String categoryDayCount = infoJson.getAsString("day_count");
//				
//				log.info("categorySource: "+categorySource);
//				log.info("categoryValue: "+categoryValue);
//				log.info("categoryUrl: "+categoryUrl);
//				log.info("categoryDayCount: "+categoryDayCount);
//				
//				String urlToMd5 = getMD5(categoryUrl);
//				DBObject dbObject = queryClassUrlThirdAdclass(urlToMd5);		//查mongo有無資料
//				if (dbObject != null) {
//					//如果mongo已有第3層資料，直接將array塞進dmpDataBean的Prod_class_info
//					thirdCategoryList = (ArrayList<String>) dbObject.get("prod_class_info");
//				}else{
//					//先檢查MYSQL有無此第1第2分類代號，沒有就不新增
//					int length =16;
//					String categoryValueStr = categoryValue;
//					categoryValueStr = categoryValueStr.substring(0,8);
//					String level2Category =categoryValueStr+String.format("%1$0"+(length -categoryValueStr.length())+"d",0);
//					int parent_id = this.categoryDAO.querySecondCategoryExist(level2Category);
//					log.info("parent_id : "+parent_id);
//					
//					if (parent_id==0){
//						log.info("NO Second Category : "+parent_id);
//						continue;
//					}
//					
//					//打爬蟲要title
//					String prodTitle = adCrawlerGetTitle(categoryUrl);
//					if ( StringUtils.isNotBlank(prodTitle)){
//						//比對title是否有命中第3分類對照表(ThirdAdClassTable.txt)
//	//					for (String string : DmpLogMapper.prodFileList) {
//						
//						log.info("ThirdCategoryLogMapper.prodFileList.size() : "+ThirdCategoryLogMapper.prodFileList.size());
//						
//						ArrayList<String> newMongothirdCateList = new ArrayList<String>();  //如此url在mongo沒有第3分類資料，即新增
//						for (String prodName : ThirdCategoryLogMapper.prodFileList) {
//						
//							log.info("prodName loop : "+prodName);
//							
//							if ( prodTitle.indexOf(prodName) > -1){
//								
//								log.info("match prodName -1 : "+prodName);
//								
//								//查詢mysql是否有第3分類資料
//								categoryValueStr = categoryValue.substring(0,8);
//								String Level3code= this.categoryDAO.queryThirdCategoryExist(categoryValueStr, prodName);
//								if (StringUtils.isBlank(Level3code)){
//									log.info("No queryThirdCategoryExist");
//									//此第2分類下沒有這個第3分類商品，新增至pfp_ad_category_new table
//	//								8.檢查mysql pfp_ad_category_new 第一第二層之下有無此關鍵字
//	//								9.沒有的話給一個第三層代號，並建立一筆第三層代號
//	//								10.yes.txt 可能會比對中多個字,每一個字均要建立第三層(第3層為8碼流水號ex:00000001)
//	//								11.將建立的多個第三層代號，用url 當key 寫入 mongodb 讓下次第4部驟可以查詢到
//	//								12.將1 2 3 層資料傳遞到下一個模組
//	//								13.kafa 紀錄格式 prod_class_info category可傳入多個分類
//									int seq = this.sequenceDAO.querySequence();
//									seq = seq+1;
//									String thirdCategorySeq = String.format("%08d", seq);
//									String thirdCategoryCode = categoryValueStr+thirdCategorySeq;
//									
//									this.categoryDAO.insertThirdCategory(Integer.toString(parent_id), thirdCategoryCode, prodName, 3);
//									this.sequenceDAO.updateSequence(seq);
//									
//									thirdCategoryList.add(thirdCategoryCode);
//									newMongothirdCateList.add(thirdCategoryCode);
//								}else{
//									thirdCategoryList.add(Level3code);
//									newMongothirdCateList.add(Level3code);
//									log.info("Have the  queryThirdCategoryExist>>");
//								}
//							}
//						}
//						
//						//有中到第3分類檔(ThirdAdClassTable.txt)，即把資料新增至class_url_third_adclass table
//						if (newMongothirdCateList.size()>0){
//							insertClassUrlThirdAdclass(urlToMd5,newMongothirdCateList);
//						} 
//					}
//				}
//				
//				//新增prod_class_info 第3分類array內容
//				JSONObject prodClassinfoObj = new JSONObject();
//				prodClassinfoObj.put("source", categorySource);
//				prodClassinfoObj.put("value", thirdCategoryList);
//				prodClassinfoObj.put("day_count", categoryDayCount);
//				prodClassinfoAry.add(prodClassinfoObj);	//prod_class_info
//			}
//			
//			//重組第3分類json後發送給kafka
//			JSONObject newALLObj = new JSONObject();
//			JSONObject newKeyObj = new JSONObject();
//			newKeyObj.put("uuid",  ((JSONObject) jsonObjOrg.get("key")).get("uuid"));
//			newKeyObj.put("memid", ((JSONObject) jsonObjOrg.get("key")).get("memid"));
//			newALLObj.put("key", newKeyObj);
//			
//			JSONObject newDataObj = new JSONObject();
//			newDataObj.put("prod_class_info", prodClassinfoAry);
//			newDataObj.put("record_date", ((JSONObject) jsonObjOrg.get("data")).get("record_date"));
//			newALLObj.put("data", newDataObj);
//			
//			
//			log.info(">>>>>>>>>>>第3分類>>>>>>:"+newALLObj.toString());
//			
//			keyOut.set(newALLObj.toString());
//			context.write(keyOut, valueOut);
//		} catch (Throwable e) {
////			log.error(">>>>>> reduce error redis key:" +reducerMapKey.toString());
//			log.error("ThirdCategoryLogReducer reduce error>>>>>> " +e);
//		}
	}
	
	public String getMD5(String str) {
        MessageDigest md = null;
	    try {
	    	md = MessageDigest.getInstance("MD5");
	        md.update(str.getBytes());
	    } catch (Exception e) {
	    	log.error(e);
	    }
		return  new BigInteger(1, md.digest()).toString(16);
	}
	
	public DBObject queryClassUrlThirdAdclass(String urlToMd5) throws Exception {
		this.dBCollection= this.mongoOrgOperations.getCollection("class_url_third_adclass");
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("_id", urlToMd5));
		andQuery.put("$and", obj);
		DBObject dbObject =  this.dBCollection.findOne(andQuery);
		return dbObject;
	}
	
	public void insertClassUrlThirdAdclass(String urlToMd5,ArrayList<String> newMongothirdCateList) throws Exception {
		this.dBCollection= this.mongoOrgOperations.getCollection("class_url_third_adclass");
	    DBObject documents = new BasicDBObject("_id",urlToMd5).append("prod_class_info", newMongothirdCateList);
	    this.dBCollection.insert(documents);
	}
	
	
	public String adCrawlerGetTitle(String url) throws Exception {
		crawlerCount = crawlerCount +1;
		
		log.info(">>>>>>crawlerCount : "+crawlerCount);
		
		if (crawlerCount == 6){
			Thread.sleep(5000);
			log.info(">>>>>>Crawler sleep(5000)");
		}
		
		//第3層資料沒有在mongo中，打爬蟲get標題
		String prodTitle = "";
		StringBuffer adCrawlerResult = HttpUtil.getInstance().doGet("http://pysvr.mypchome.com.tw/product/?url="+url);
		
		log.info(">>>>>>Crawler : "+"http://pysvr.mypchome.com.tw/product/?url="+url);
		
		JSONObject adCrawlerObj = (net.minidev.json.JSONObject)jsonParser.parse(adCrawlerResult.toString());
		JSONArray adCrawlerAry = (JSONArray) adCrawlerObj.get("products");
		for (Object adCrawlerObjs : adCrawlerAry) {
			JSONObject adCrawler = (JSONObject) adCrawlerObjs;
			prodTitle = adCrawler.getAsString("title");
			log.info(">>>>>> url title : "+prodTitle);
		}
		
		return prodTitle;
	}
	
	public void cleanup(Context context) {
		
		this.categoryDAO.closeAll();
		this.sequenceDAO.closeAll();
//		try {
////			log.info(">>>>>>write cleanup>>>>>");
//			
//			String kafkaTopic;
//			String env = context.getConfiguration().get("spring.profiles.active");
//			if(env.equals("prd")){
//				kafkaTopic = "dmp_log_prd";
//			}else{
//				kafkaTopic = "dmp_log_stg";
//			}
//			log.info(">>>>>>kafkaTopic: " + kafkaTopic);
//			
//			Iterator iterator = kafkaDmpMap.entrySet().iterator();
//			while (iterator.hasNext()) {
//				count = count+1;
//				Map.Entry mapEntry = (Map.Entry) iterator.next();
//				Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>(kafkaTopic, "", mapEntry.getValue().toString()));
//				while (!f.isDone()) {
//				}
//				keyOut.set(mapEntry.getValue().toString());
//				context.write(keyOut, valueOut);
////				log.info(">>>>>>reduce Map send kafka:" + mapEntry.getValue().toString());
//			}
//			producer.close();
//			log.info(">>>>>>reduce count:" + count);
//			
//			log.info(">>>>>>write clssify to Redis>>>>>");
//			log.info(">>>>>>cleanup redisClassifyMap:" + redisClassifyMap);
//			for (Entry<String, Integer> redisMap : redisClassifyMap.entrySet()) {
//				String redisKey = redisMap.getKey();
//				int count = redisMap.getValue();
//				redisTemplate.opsForValue().increment(redisKey, count);
//				redisTemplate.expire(redisKey, 4, TimeUnit.DAYS);
//			}
//			
//		} catch (Throwable e) {
//			log.error("reduce cleanup error>>>>>> " + e);
//		}
	}

	
	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		
//		DmpLogReducer dmpLogReducer = ctx.getBean(DmpLogReducer.class);
////		String a ="{'key':{'memid':'null','uuid':'c014b82c-65c3-4e59-88ac-825152412307'},'data':{'category_info':{'source':'null','value':'null'},'sex_info':{'source':'null','value':'null'},'age_info':{'source':'null','value':'null'},'area_country_info':{'source':'ip','value':'Taiwan'},'area_city_info':{'source':'ip','value':'Chengde'},'device_info':{'source':'user-agent','value':'MOBILE'},'device_phone_info':{'source':'user-agent','value':'APPLE'},'device_os_info':{'source':'user-agent','value':'IOS'},'device_browser_info':{'source':'user-agent','value':'Mobile Safari'},'time_info':{'source':'datetime','value':'17'},'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'Y'},{'all_kdcl_log_time_info':'Y'}]},'url':'','ip':'101.139.176.46','record_date':'2018-06-11','org_source':'kdcl','date_time':'2018-05-22 17:07:44','user_agent':'Mozilla\\5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) AppleWebKit\\604.1.34 (KHTML, like Gecko) GSA\\37.1.171590344 Mobile\\15E302 Safari\\604.1','ad_class':'0017024822530000','record_count':658}";
//		String a ="{'key':{'memid':'null','uuid':'c014b82c-65c3-4e59-88ac-825152412307'},'data':{'category_info':{'source':24h,'value':7997979979},'sex_info':{'source':'null','value':'null'},'age_info':{'source':'null','value':'null'},'area_country_info':{'source':'ip','value':'Taiwan'},'area_city_info':{'source':'ip','value':'Chengde'},'device_info':{'source':'user-agent','value':'MOBILE'},'device_phone_info':{'source':'user-agent','value':'APPLE'},'device_os_info':{'source':'user-agent','value':'IOS'},'device_browser_info':{'source':'user-agent','value':'Mobile Safari'},'time_info':{'source':'datetime','value':'17'},'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'Y'},{'all_kdcl_log_time_info':'Y'}]},'url':'http:\\\\www.gaomei.com.tw\\openhours\','ip':'101.139.176.46','record_date':'2018-06-11','org_source':'kdcl','date_time':'2018-05-22 17:05:53','user_agent':'Mozilla\\5.0 (iPhone; CPU iPhone OS 11_3_1 like Mac OS X) AppleWebKit\\604.1.34 (KHTML, like Gecko) GSA\\37.1.171590344 Mobile\\15E302 Safari\\604.1','ad_class':'0017024822530000','record_count':287}";
//		JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
//		net.minidev.json.JSONObject json = (net.minidev.json.JSONObject) jsonParser.parse(a);
//		for (int i = 0; i < 1; i++) {
//			dmpLogReducer.reduce(new Text(json.toString()), null, null);
//		}	
		
		
//		RedisTemplate<String, Object> redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
//		System.out.println(redisTemplate.opsForValue().get("kdcl_null_d9228b87-bc6d-4e23-a4e6-0d7563380c7e"));
		
//		redisTemplate.delete("kdcl_null_c014b82c-65c3-4e59-88ac-825152412307");
//		List<String> a = new ArrayList<>();
//		a.add("a");
//		a.add("b");
//		a.add("c");
//		
		
//		StringBuffer f = new StringBuffer();
//		f.append("ALEX");
//		
//		
//		String c = f.toString();
//		
//		f.setLength(0);
//		
//		System.out.println(c);
		
//		RedisTemplate<String, Object> redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
////		redisTemplate.opsForValue().set("alex", "123");
//		redisTemplate.expire("alex", 60, TimeUnit.SECONDS);
//		System.out.println(redisTemplate.opsForValue().get("alex"));
		
		
//		String[] array = {"alex","TEST"};
			
		
		
		
		
//		System.out.println(json);
//		JSONObject v = new JSONObject(a);
//		System.out.println(v);

//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
}

//	
}
