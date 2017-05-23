package com.pchome.dmp.mapreduce.crawlbreadcrumb;

import java.util.ArrayList;
import java.util.Date;
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


import net.minidev.json.JSONObject;

public class CrawlBreadCrumbReducerTest extends Reducer<Text, Text, Text, Text> {

	private Log log = LogFactory.getLog(this.getClass());

	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;

	@Override
	public void setup(Context context) {

		try {

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
			producer = new KafkaProducer<String, String>(props);

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {

			log.info("key:" + key.toString());
			Date date = new Date();
			for (Text url : value) {
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>" + key);
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>" + value);
				
				JSONObject json = new JSONObject();
				json.put("url", url.toString());
				json.put("status", (key.toString().matches("\\d{16}")?"1":"0"));
				json.put("ad_class", key.toString().matches("\\d{16}")?key.toString():"");
				json.put("create_date", date);
				json.put("update_date", date);
				kafkaList.add(json);
			}
		} catch (Exception e) {
			log.error("error" + e);
		}

	}

	@Override
	public void cleanup(Context context) {
		try {
			
			Future<RecordMetadata> f  = producer.send(new ProducerRecord<String, String>("TEST", "", kafkaList.toString()));
			while (!f.isDone()) {
			}
			
			// coll.insert(list);
			// mongoClient.close();
			// Future<RecordMetadata> f = producer.send(new
			// ProducerRecord<String, String>("TEST",
			// "",kafkaList.toArray().toString()));
			// while (!f.isDone()) {
			// }

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}

}
