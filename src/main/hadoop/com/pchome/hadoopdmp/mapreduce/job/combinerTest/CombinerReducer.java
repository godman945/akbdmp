package com.pchome.hadoopdmp.mapreduce.job.combinerTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.json.JSONArray;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class CombinerReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("CombinerReducer");
	
	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	
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

	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;

	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
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
			log.error("reduce setup error>>>>>> " +e);
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		// 0:memid + 1:uuid + 2:category + 3.categorySource
		// 4.sex + 5.sexSource + 6.age + 7.ageSource
		// 8.country + 9.city + 10.areaInfoSource
		// 11.device_info_source + 12.device_info
		// 13.device_phone_info + 14.device_os_info + 15.device_browser_info
		// 16.time_info_hour + 17.time_info_source

		// classify
		// 18.personal_info_api + 19.personal_info
		// 20.class_ad_click + 21.class_24h_url + 22.class_ruten_url
		// 23.area_info + 24.device_info + 25.time_info
		//26.url + 27.ip + 28.record_date + 29.org_source(kdcl、campaign) 
		//30.date_time + 31.user_agent +32.ad_class + 33.record_count
		try {
//			log.info(">>>>>> reduce start : " + key);

			String data = key.toString();
			
			
//			Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>("dmp_log_prd", "", data));
//			while (!f.isDone()) {
//			}
			
			log.info(">>>>>>reduce write key:" + data);
			
			keyOut.set(data);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error("reduce error>>>>>> " +e);
		}

	}

	public void cleanup(Context context) {
		try {
			producer.close();
		} catch (Exception e) {
			log.error("reduce cleanup error>>>>>> " +e);
		}
	}

	
//	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//}

//	
}
