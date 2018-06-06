package com.pchome.hadoopdmp.spring.config.bean.kafka;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
	
	@Value("${kafka.metadata.broker.list}")
	private  List<String> brokerList;
	@Value("${kafka.acks}")
	private  String acks;
	@Value("${kafka.retries}")
	private  int retries;
	@Value("${kafka.batch.size}")
	private  int batchSize;
	@Value("${kafka.linger.ms}")
	private  int lingerMs;
	@Value("${kafka.buffer.memory}")
	private  int bufferMemory;
	@Value("${kafka.serializer.class}")
	private  String serializerClass;
	@Value("${kafka.key.serializer}")
	private  String keySerializer;
	@Value("${kafka.value.serializer}")
	private  String valueSerializer;
	
	public Properties kafkaProperties() {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("acks", acks);
		props.put("retries", retries);
		props.put("batch.size", batchSize);
		props.put("linger.ms", lingerMs);
		props.put("buffer.memory", bufferMemory);
		props.put("key.serializer", keySerializer);
		props.put("value.serializer", valueSerializer);
	    return props;
	}

	@Bean(name = "kafkaProducer")
	public Producer<String, String>  kafkaProducer() {
	    return new KafkaProducer<String, String>(kafkaProperties());
	}
}
