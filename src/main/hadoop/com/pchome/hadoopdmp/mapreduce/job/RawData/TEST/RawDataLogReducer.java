package com.pchome.hadoopdmp.mapreduce.job.RawData.TEST;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.IKdclStatisticsSourceService;
import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.KdclStatisticsSourceService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class RawDataLogReducer extends Reducer<Text, Text, Text, Text> {

	Log log = LogFactory.getLog("CategoryLogReducer");

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
	
	private String environment;
	
	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;

	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {
//			this.sdf = new SimpleDateFormat("yyyy-MM-dd");
//			if(StringUtils.isNotBlank(context.getConfiguration().get("spring.profiles.active"))){
//				System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
//				this.environment = "prd";
//			}else{
//				System.setProperty("spring.profiles.active", "stg");
//				this.environment = "stg";
//			}
//			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//			this.kdclStatisticsSourceService = ctx.getBean(KdclStatisticsSourceService.class);
//			this.kafkaMetadataBrokerlist = ctx.getEnvironment().getProperty("kafka.metadata.broker.list");
//			this.kafkaAcks = ctx.getEnvironment().getProperty("kafka.acks");
//			this.kafkaRetries = ctx.getEnvironment().getProperty("kafka.retries");
//			this.kafkaBatchSize = ctx.getEnvironment().getProperty("kafka.batch.size");
//			this.kafkaLingerMs = ctx.getEnvironment().getProperty("kafka.linger.ms");
//			this.kafkaBufferMemory = ctx.getEnvironment().getProperty("kafka.buffer.memory");
//			this.kafkaSerializerClass = ctx.getEnvironment().getProperty("kafka.serializer.class");
//			this.kafkaKeySerializer = ctx.getEnvironment().getProperty("kafka.key.serializer");
//			this.kafkaValueSerializer = ctx.getEnvironment().getProperty("kafka.value.serializer");
//			
//			Properties props = new Properties();
//			props.put("bootstrap.servers", kafkaMetadataBrokerlist);
//			props.put("acks", kafkaAcks);
//			props.put("retries", kafkaRetries);
//			props.put("batch.size", kafkaBatchSize);
//			props.put("linger.ms", kafkaLingerMs);
//			props.put("buffer.memory", kafkaBufferMemory);
//			props.put("serializer.class", kafkaSerializerClass);
//			props.put("key.serializer", kafkaKeySerializer);
//			props.put("value.serializer", kafkaValueSerializer);
//			producer = new KafkaProducer<String, String>(props);

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
//			String data[] = key.toString().split(SYMBOL);
//			log.info(">>>>>>reduce write key:" + key);
			
			keyOut.set(key);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.info("reduce error"+e.getMessage());
			log.error(key, e);
		}

	}

	public void cleanup(Context context) {
	}

	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		
//		int a = 0;
//		Calendar calendar = Calendar.getInstance();  
//		if(calendar.get(Calendar.HOUR_OF_DAY) == 16){
//			calendar.add(Calendar.DAY_OF_MONTH, -1); 
//			System.out.println(sdf.format(calendar.getTime()));;
//		}
//		KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("7", sdf.format(calendar.getTime()));
//		a = a + kdclStatisticsSource.getCounter();
//		
//		System.out.println(a);
//		
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("7", sdf.format(calendar.getTime()));
//		
//		
//		a = a + 10;
//		
		
		
//		String recodeDate = sdf.format(date);
//		System.out.println(recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ad_click", recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("24h", recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ruten", recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("personal_info", recodeDate);
		
		
//		UserDetailService userDetailService = (UserDetailService) ctx.getBean(UserDetailService.class);
//		UserDetailMongoBean userDetailMongoBean = userDetailService.findUserId("zxc910615");
//		
//		System.out.println(userDetailMongoBean.get_id());
//		System.out.println((String)userDetailMongoBean.getUser_info().get("sex"));
		
		
		
		
		
		
		
//		Query query = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
//		UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(query, UserDetailMongoBean.class);
//		userDetailMongoBean.getUser_info().get("sex")
//		userDetailMongoBean.getUser_info().get("age")
		
		
		
		
//		System.out.println(userDetailMongoBean.getCategory_info().get(0).get("category"));
		
		
//		KdclStatisticsSource kdclStatisticsSource = new KdclStatisticsSource();
//		kdclStatisticsSource.setClassify("A");
//		kdclStatisticsSource.setIdType("alex");
//		kdclStatisticsSource.setServiceType("9");
//		kdclStatisticsSource.setBehavior("GGG");
//		kdclStatisticsSource.setCounter(0);
//		kdclStatisticsSource.setRecordDate("2018-01-25");
//		kdclStatisticsSource.setUpdateDate(new Date());
//		kdclStatisticsSource.setCreateDate(new Date());
//		kdclStatisticsSourceService.save(kdclStatisticsSource);
	}


}
