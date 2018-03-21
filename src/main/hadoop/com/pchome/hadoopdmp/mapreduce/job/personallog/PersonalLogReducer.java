package com.pchome.hadoopdmp.mapreduce.job.personallog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.hadoopdmp.enumerate.EnumKdclStatisticsSource;
import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.IKdclStatisticsSourceService;
import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.KdclStatisticsSourceService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class PersonalLogReducer extends Reducer<Text, Text, Text, Text> {

	Log log = LogFactory.getLog("PersonalLogReducer");

	SimpleDateFormat sdf = null;
	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	private int uuid24ClassifyIsY = 0;
	private int uuid24ClassifyIsN = 0;
	private int memid24ClassifyIsY = 0;
	private int memid24ClassifyIsN = 0;
	private int uuidRutenClassifyIsY = 0;
	private int uuidRutenClassifyIsN = 0;
	private int memidRutenClassifyIsY = 0;
	private int memidRutenClassifyIsN = 0;
	private int memidAdclickClassifyIsY = 0;
	private int uuidAdclickClassifyIsY = 0;
	private int userInfoClassifyIsY = 0;
	private int userInfoClassifyIsN = 0;
	
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

	private IKdclStatisticsSourceService kdclStatisticsSourceService;
	
	private List<JSONObject> kafkaList = new ArrayList<>();

	private Producer<String, String> producer = null;

	private Date date = new Date();
	
	private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	
	private String environment;
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {
			this.sdf = new SimpleDateFormat("yyyy-MM-dd");
			if(StringUtils.isNotBlank(context.getConfiguration().get("spring.profiles.active"))){
				System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
				this.environment = "prd";
			}else{
				System.setProperty("spring.profiles.active", "stg");
				this.environment = "stg";
			}
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.kdclStatisticsSourceService = ctx.getBean(KdclStatisticsSourceService.class);
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

		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		// 0 : Memid
		// 1 : Uuid
		// 2 : AdClass
		// 3 : Age
		// 4 : Sex
		// 5 : Source (ad_click、24h、ruten)
		// 6 : RecodeDate
		// 7 : Type(memid or uuid)
		try {
//			log.info(">>>>>> reduce start : " + key);
			String data[] = key.toString().split(SYMBOL);

			JSONObject json = new JSONObject();
			json.put("memid", data[0]);
			json.put("uuid", data[1]);
			json.put("adClass", data[2]);
			json.put("age", data[3]);
			json.put("sex", data[4]);
			json.put("source", data[5]);
			json.put("recordDate", data[6]);

			keyOut.set(key);
			context.write(keyOut, valueOut);
			
			if(key.toString().indexOf("uuid_24h_Y") >=0 ){
				uuid24ClassifyIsY = uuid24ClassifyIsY + 1;
			}
			if(key.toString().indexOf("uuid_24h_N") >=0 ){
				uuid24ClassifyIsN = uuid24ClassifyIsN + 1;
			}
			if(key.toString().indexOf("memid_24h_Y") >=0 ){
				memid24ClassifyIsY = memid24ClassifyIsY + 1;
			}
			if(key.toString().indexOf("memid_24h_N") >=0 ){
				memid24ClassifyIsN = memid24ClassifyIsN + 1;
			}
			if(key.toString().indexOf("uuid_ruten_Y") >=0 ){
				uuidRutenClassifyIsY = uuidRutenClassifyIsY + 1;
			}
			if(key.toString().indexOf("uuid_ruten_N") >=0 ){
				uuidRutenClassifyIsN = uuidRutenClassifyIsN + 1;
			}
			if(key.toString().indexOf("memid_ruten_Y") >=0 ){
				memidRutenClassifyIsY = memidRutenClassifyIsY + 1;
			}
			if(key.toString().indexOf("memid_ruten_N") >=0 ){
				memidRutenClassifyIsN = memidRutenClassifyIsN + 1;
			}
			if(key.toString().indexOf("memid_adclick_Y") >=0 ){
				memidAdclickClassifyIsY = memidAdclickClassifyIsY + 1;
			}
			if(key.toString().indexOf("uuid_adclick_Y") >=0 ){
				uuidAdclickClassifyIsY = uuidAdclickClassifyIsY + 1;
			}
			if(key.toString().indexOf("user_info_Classify_Y") >=0 ){
				userInfoClassifyIsY = userInfoClassifyIsY + 1;
			}
			if(key.toString().indexOf("user_info_Classify_N") >=0 ){
				userInfoClassifyIsN = userInfoClassifyIsN + 1;
			}
			
			context.write(new Text(data[7]), new Text("1"));
		} catch (Exception e) {
			log.info("reduce error"+e.getMessage());
			log.error(key, e);
		}

	}

	public void cleanup(Context context) {
		try {
			log.info("------------ cleanup start ------------");
			if(this.environment.equals("prd")){
				System.setProperty("spring.profiles.active", "prd");
			}else{
				System.setProperty("spring.profiles.active", "stg");
			}
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.kdclStatisticsSourceService = ctx.getBean(KdclStatisticsSourceService.class);
			
			String recodeDate = "";
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, -1); 
			if(calendar.get(Calendar.HOUR_OF_DAY) == 0){
				calendar.add(Calendar.DAY_OF_MONTH, -1); 
				recodeDate = sdf1.format(calendar.getTime());
			}else{
				recodeDate = sdf1.format(calendar.getTime());
			}
			
			kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ad_click", recodeDate);
			kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("24h", recodeDate);
			kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ruten", recodeDate);
			kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("personal_info", recodeDate);
			
			for (EnumKdclStatisticsSource enumKdclStatisticsSource : EnumKdclStatisticsSource.values()) {
				if(enumKdclStatisticsSource.getKey().equals("MEMID_24_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("24h",recodeDate,"memid","Y");
					if(kdclStatisticsSource != null){
//						log.info("memid24ClassifyIsY:"+memid24ClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						memid24ClassifyIsY = memid24ClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(memid24ClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","kdcl","24h","Y",memid24ClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("MEMID_24_N")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("24h",recodeDate,"memid","N");
					if(kdclStatisticsSource != null){
//						log.info("memid24ClassifyIsN:"+memid24ClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						memid24ClassifyIsN = memid24ClassifyIsN + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(memid24ClassifyIsN);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","kdcl","24h","N",memid24ClassifyIsN,recodeDate,date,kdclStatisticsSourceService);	
					}
					
				}
				if(enumKdclStatisticsSource.getKey().equals("UUID_24_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("24h",recodeDate,"uuid","Y");
					if(kdclStatisticsSource != null){
//						log.info("uuid24ClassifyIsY:"+uuid24ClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						uuid24ClassifyIsY = uuid24ClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(uuid24ClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("uuid","kdcl","24h","Y",uuid24ClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("UUID_24_N")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("24h",recodeDate,"uuid","N");
					if(kdclStatisticsSource != null){
//						log.info("uuid24ClassifyIsN:"+uuid24ClassifyIsN);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						uuid24ClassifyIsN = uuid24ClassifyIsN + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(uuid24ClassifyIsN);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("uuid","kdcl","24h","N",uuid24ClassifyIsN,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("MEMID_RUTEN_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("ruten",recodeDate,"memid","Y");
					if(kdclStatisticsSource != null){
//						log.info("memidRutenClassifyIsY:"+memidRutenClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						memidRutenClassifyIsY = memidRutenClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(memidRutenClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","kdcl","ruten","Y",memidRutenClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("MEMID_RUTEN_N")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("ruten",recodeDate,"memid","N");
					if(kdclStatisticsSource != null){
//						log.info("memidRutenClassifyIsN:"+memidRutenClassifyIsN);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						memidRutenClassifyIsN = memidRutenClassifyIsN + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(memidRutenClassifyIsN);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","kdcl","ruten","N",memidRutenClassifyIsN,recodeDate,date,kdclStatisticsSourceService);	
					}
					
				}
				if(enumKdclStatisticsSource.getKey().equals("UUID_RUTEN_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("ruten",recodeDate,"uuid","Y");
					if(kdclStatisticsSource != null){
//						log.info("uuidRutenClassifyIsY:"+uuidRutenClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						uuidRutenClassifyIsY = uuidRutenClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(uuidRutenClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("uuid","kdcl","ruten","Y",uuidRutenClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("UUID_RUTEN_N")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("ruten",recodeDate,"uuid","N");
					if(kdclStatisticsSource != null){
//						log.info("uuidRutenClassifyIsN:"+uuidRutenClassifyIsN);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						uuidRutenClassifyIsN = uuidRutenClassifyIsN + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(uuidRutenClassifyIsN);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("uuid","kdcl","ruten","N",uuidRutenClassifyIsN,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("UUID_ADCLICK_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("ad_click",recodeDate,"uuid","Y");
					if(kdclStatisticsSource != null){
//						log.info("uuidAdclickClassifyIsY:"+uuidAdclickClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						uuidAdclickClassifyIsY = uuidAdclickClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(uuidAdclickClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("uuid","kdcl","ad_click","Y",uuidAdclickClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
					
				}
				if(enumKdclStatisticsSource.getKey().equals("MEMID_ADCLICK_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("ad_click",recodeDate,"memid","Y");
					if(kdclStatisticsSource != null){
//						log.info("memidAdclickClassifyIsY:"+memidAdclickClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						memidAdclickClassifyIsY = memidAdclickClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(memidAdclickClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","kdcl","ad_click","Y",memidAdclickClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("user_info_Classify_Y")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("personal_info",recodeDate,"memid","Y");
					if(kdclStatisticsSource != null){
//						log.info("userInfoClassifyIsY:"+userInfoClassifyIsY);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						userInfoClassifyIsY = userInfoClassifyIsY + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(userInfoClassifyIsY);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","member","personal_info","Y",userInfoClassifyIsY,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				if(enumKdclStatisticsSource.getKey().equals("user_info_Classify_N")){
					KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("personal_info",recodeDate,"memid","N");
					if(kdclStatisticsSource != null){
//						log.info("userInfoClassifyIsN:"+userInfoClassifyIsN);
//						log.info("kdclStatisticsSource.getCounter():"+kdclStatisticsSource.getCounter());
//						log.info("----------------------------");
						userInfoClassifyIsN = userInfoClassifyIsN + kdclStatisticsSource.getCounter();
						kdclStatisticsSource.setCounter(userInfoClassifyIsN);
						kdclStatisticsSourceService.saveOrUpdate(kdclStatisticsSource);
					}else{
						savekdclStatisticsSource("memid","member","personal_info","N",userInfoClassifyIsN,recodeDate,date,kdclStatisticsSourceService);	
					}
				}
				
			}
			log.info("------------ cleanup end ------------");
			producer.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	public void savekdclStatisticsSource(String idType,String serviceType,String behavior,String classify ,int count,String recodeDate,Date date,IKdclStatisticsSourceService kdclStatisticsSourceService) throws Exception{
		KdclStatisticsSource kdclStatisticsSource = new KdclStatisticsSource();
		kdclStatisticsSource.setIdType(idType);
		kdclStatisticsSource.setServiceType(serviceType);
		kdclStatisticsSource.setClassify(classify);
		kdclStatisticsSource.setBehavior(behavior);
		kdclStatisticsSource.setCounter(count);
		kdclStatisticsSource.setRecordDate(recodeDate);
		kdclStatisticsSource.setUpdateDate(date);
		kdclStatisticsSource.setCreateDate(date);
		kdclStatisticsSourceService.save(kdclStatisticsSource);
	}

}
