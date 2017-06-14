package com.pchome.dmp.mapreduce.job.categorylog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.mongodb.DBObject;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.dmp.dao.sql.KdclStatisticsSourceDAO;
import com.pchome.dmp.enumerate.EnumCategoryJob;
import com.pchome.dmp.enumerate.EnumKdclCkDailyAddedAmount;
import com.pchome.dmp.enumerate.EnumKdclPvDailyAddedAmount;
import com.pchome.dmp.factory.job.AncestorJob;
import com.pchome.dmp.factory.job.FactoryCategoryJob;
import com.pchome.soft.depot.utils.KafkaUtil;

@Component
public class CategoryLogReducer extends Reducer<Text, Text, Text, Text> {

	private final static String SYMBOL = String.valueOf(new char[]{9, 31});
//	private Log log = LogFactory.getLog(this.getClass());
	
	Log log = LogFactory.getLog(this.getClass());

	public static String record_date;

	public AncestorJob job = null;
	
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

	@Override
	public void setup(Context context) {
		try {
    		System.setProperty("spring.profiles.active", "prd");
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

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {
			
			String data[] = key.toString().split(SYMBOL);
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
    		Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "", kafkaList.toString()));
			while (!f.isDone()) {
			}
    		
    	} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

}
