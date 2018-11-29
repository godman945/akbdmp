package com.pchome.hadoopdmp.mapreduce.job.pacllog;

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
public class PaclLogConvertCombiner extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("PaclLogConvertCombiner");
	
	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	public static String record_date;

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
//	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;

	public void setup(Context context) {
		log.info("********setup********");
		try {
			
		} catch (Exception e) {
			log.error("MyCombiner setup error>>>>>> " +e);
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {
			log.info(">>>>>>>>>key:"+key.toString());
			
//			keyOut.set(sendKafkaJson.toString());
//			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error("reduce error>>>>>> " +e);
		}

	}

	public void cleanup(Context context) {
//		try {
//			producer.close();
//		} catch (Exception e) {
//			log.error("reduce cleanup error>>>>>> " +e);
//		}
	}

	
//	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "local");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//}

//	
}
