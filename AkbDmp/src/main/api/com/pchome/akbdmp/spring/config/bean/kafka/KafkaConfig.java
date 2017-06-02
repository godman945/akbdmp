//package com.pchome.akbdmp.spring.config.bean.kafka;
//
//import java.util.Properties;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.PropertySource;
//import org.springframework.core.env.Environment;
//
//@Configuration
//@PropertySource({ "classpath:config/prop/${spring.profiles.active}/kafka.properties" })
//public class KafkaConfig {
//	
//	@Autowired
//	private Environment env;
//	
//	public Properties kafkaProperties() {
//		Properties props = new Properties();
//		props.put("bootstrap.servers", env.getProperty("kafka.metadata.broker.list"));
//		props.put("acks", "all");
//		props.put("retries", 0);
//		props.put("batch.size", 16384);
//		props.put("linger.ms", 1);
//		props.put("buffer.memory", 536870912);
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//	    return props;
//	}
//
//	@Bean(name = "kafkaProducer")
//	public Producer<String, String>  kafkaProducer() {
//	    return new KafkaProducer<String, String>(kafkaProperties());
//	}
//}
