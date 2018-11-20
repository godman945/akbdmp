//package com.pchome.hadoopdmp.mapreduce.job.Geoip2;
//
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.Properties;
//import java.util.concurrent.Future;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.codehaus.jettison.json.JSONObject;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.AnnotationConfigApplicationContext;
//import org.springframework.stereotype.Component;
//
//import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.IKdclStatisticsSourceService;
//import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.KdclStatisticsSourceService;
//import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
//
//@Component
//public class Geoip2LogReducer extends Reducer<Text, Text, Text, Text> {
//
//	Log log = LogFactory.getLog("Geoip2LogReducer");
//
//	SimpleDateFormat sdf = null;
//	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
//	private int uuid24ClassifyIsY = 0;
//	private int uuid24ClassifyIsN = 0;
//	private int memid24ClassifyIsY = 0;
//	private int memid24ClassifyIsN = 0;
//	private int uuidRutenClassifyIsY = 0;
//	private int uuidRutenClassifyIsN = 0;
//	private int memidRutenClassifyIsY = 0;
//	private int memidRutenClassifyIsN = 0;
//	private int memidAdclickClassifyIsY = 0;
//	private int uuidAdclickClassifyIsY = 0;
//	private int userInfoClassifyIsY = 0;
//	private int userInfoClassifyIsN = 0;
//	
//	private Text keyOut = new Text();
//	private Text valueOut = new Text();
//	public static String record_date;
//
//	private String kafkaMetadataBrokerlist;
//
//	private String kafkaAcks;
//
//	private String kafkaRetries;
//
//	private String kafkaBatchSize;
//
//	private String kafkaLingerMs;
//
//	private String kafkaBufferMemory;
//
//	private String kafkaSerializerClass;
//
//	private String kafkaKeySerializer;
//
//	private String kafkaValueSerializer;
//
//	private IKdclStatisticsSourceService kdclStatisticsSourceService;
//	
//	private String environment;
//	
//	List<JSONObject> kafkaList = new ArrayList<>();
//
//	Producer<String, String> producer = null;
//
//	public void setup(Context context) {
//		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
//		try {
//
//		} catch (Exception e) {
//			log.error(e.getMessage());
//		}
//	}
//
//	@Override
//	public void reduce(Text key, Iterable<Text> value, Context context) {
//		try {
//			log.info(">>>>>>reduce write key:" + key);
//			
//			keyOut.set(key);
//			context.write(keyOut, valueOut);
//			
//		} catch (Exception e) {
//			log.info("reduce error"+e.getMessage());
//			log.error(key, e);
//		}
//
//	}
//
//	public void cleanup(Context context) {
//	}
//
//	
//	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//		Date date = new Date();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		
//	}
//
//
//}
