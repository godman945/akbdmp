package com.pchome.hadoopdmp.mapreduce.job.categorylog;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

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

import com.pchome.akbdmp.job.bean.ClassCountLogBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class CategoryLogReducer extends Reducer<Text, Text, Text, Text> {

	Log log = LogFactory.getLog("CategoryLogReducer");
	
//	private MongoOperations mongoOperations;
	
	private final static String SYMBOL = String.valueOf(new char[]{9, 31});
	
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
	
//	private CategoryLogReducer categoryLogReducer;

	@Override
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {
    		System.setProperty("spring.profiles.active", "prd");
    		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);

    		
    		//    		this.mongoOperations = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
    		
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
			props.put("linger.ms",kafkaLingerMs );
			props.put("buffer.memory", kafkaBufferMemory);
			props.put("serializer.class", kafkaSerializerClass);
			props.put("key.serializer", kafkaKeySerializer);
			props.put("value.serializer", kafkaValueSerializer);
			producer = new KafkaProducer<String, String>(props);
    		
    		
			
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "prd");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		CategoryLogReducer categoryLogReducer = ctx.getBean(CategoryLogReducer.class);
//		categoryLogReducer.reduce(null, null, null);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		log.info(">>>>>> reduce start : " + key);
//		System.setProperty("spring.profiles.active", "prd");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		
		// 0:Memid + 1:Uuid + 2:AdClass + 3:Age + 4:Sex + 5:Source + 6:RecodeDate + 7:Type(memid or uuid)
		try {
			
//			String key1="bessie	0011017816840000	29	F	ruten	 date	uuid";
			
			String data[] = key.toString().split(SYMBOL);
			
//			ClassCountLogBean classCountLogBean = new ClassCountLogBean();
//			classCountLogBean.setMemid(data[0]);
//			classCountLogBean.setUuid(data[1]);
//			classCountLogBean.setAdClass(data[2]);
//			classCountLogBean.setAge(data[3]);
//			classCountLogBean.setSex(data[4]);
//			classCountLogBean.setSource(data[5]);
//			classCountLogBean.setRecordDate(data[6]);
//			classCountLogBean.setType(data[7]);
			
			
			/*
ClassCountLogBean classCountLogBean = new ClassCountLogBean();
   classCountLogBean.setMemid(data[0]);
   classCountLogBean.setUuid(data[1]);
   classCountLogBean.setAdClass(data[2]);
   classCountLogBean.setAge(data[3]);
   classCountLogBean.setSex(data[4]);
   classCountLogBean.setSource(data[5]);
   classCountLogBean.setRecordDate(dateString);
   classCountLogBean.setType(data[7]);
   mongoOperations.save(classCountLogBean);
			 * */
			
			

//			JSONObject json = new JSONObject();
//			json.put("url", url.toString());
//			json.put("status", (key.toString().matches("\\d{16}") ? "1" : "0"));
//			json.put("ad_class", key.toString().matches("\\d{16}") ? key.toString() : "");
//			json.put("create_date", date);
//			json.put("update_date", date);
//			kafkaList.add(json);

			
			
//			Date date = new Date();
//			for (Text url : value) {
//				log.info("key >>>>>>>>>>>>>>>>>>>>>>" + key);
//				log.info("value >>>>>>>>>>>>>>>>>>>>>>" + value);
////				JSONObject json = new JSONObject();
////				json.put("url", url.toString());
////				json.put("status", (key.toString().matches("\\d{16}")?"1":"0"));	//(0:未分類  1:已分類  2:跳過)
////				json.put("ad_class", key.toString().matches("\\d{16}")?key.toString():"");
////				json.put("create_date", date);
////				json.put("update_date", date);
////				kafkaList.add(json);
//			}
			
			   
				
				JSONObject json = new JSONObject();
				json.put("memid", data[0]);
				json.put("uuid",data[1] );
				json.put("adClass", data[2]);
				json.put("age", data[3]);
				json.put("sex",data[4]);
				json.put("source", data[5]);
				json.put("recordDate",data[6] );
				
				
    		Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "",json.toString()));
//			while (!f.isDone()) {
//			}
			
			log.info(">>>>>> write key:" + key);
			keyOut.set(key);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error(key, e);
		}

	}

	@Override
	public void cleanup(Context context) {
		try {
    		/*mark by bessie
    		coll.insert(list);
    		mongoClient.close();
    		*/
			
			producer.close();
			
//    		Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "", kafkaList.toString()));
//			while (!f.isDone()) {
//			}
    		
    	} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

}
