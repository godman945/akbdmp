package com.pchome.soft.depot.utils;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

@Component
public class KafkaUtil {

//	Log log = LogFactory.getLog(KafkaUtil.class);
	
	public static Producer<String, String> kafkaProducer = null;
	
	 static {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka1.mypchome.com.tw:9091");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 536870912);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducer = new KafkaProducer<String, String>(props);
	}
	
	

//	@Autowired
//	private Producer<String, String> kafkaProducer;

	public static void sendMessage(String topicname, String partitionKey, String mesg) {
		try {
			Future<RecordMetadata> f = kafkaProducer.send(new ProducerRecord<String, String>(topicname, partitionKey, mesg));
//				while (!f.isDone()) {
//			}
//
//			RecordMetadata recordMetadata = f.get();

//			log.info("Topic" + recordMetadata.topic() + recordMetadata.offset() + recordMetadata.partition());

		} catch (Exception e) {

//			log.error(">>>>" + e.getMessage());

		}

	}
	
	public static void close() {
		kafkaProducer.close();
	}

//	public static void main(String[] args) {
//
//		Log log = LogFactory.getLog(KafkaUtil.class);
//
//		System.setProperty("spring.profiles.active", "local");
//
//		// System.setProperty("hadoop.home.dir",
//		// "d:\\nico_data\\hadoop\\hadoop-2.5.2\\hadoop-2.5.2");
//
//		@SuppressWarnings("resource")
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
//		KafkaUtil kafkaUtil = (KafkaUtil) ctx.getBean(KafkaUtil.class);
//		JSONObject result2 = new JSONObject();
//		result2.put("trackId", "PCSP201603230007");
//		result2.put("pcsUserId", "PCSU201605130005");
//		result2.put("trackType", "2");
//		result2.put("create", "1");
//
//		kafkaUtil.sendMessage("newsFeedTime", "newsFeedTime", "20170120");
//		// kafkaUtil.sendMessage("sample", "alex","alex messageQQ5..");
//		// kafkaUtil.kafkaClose();
//
//	}

}
