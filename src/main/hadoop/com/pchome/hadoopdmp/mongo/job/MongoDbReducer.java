package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.Producer;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class MongoDbReducer extends Reducer<Text, Text, Text, Text> {

	Log log = LogFactory.getLog(this.getClass());

	SimpleDateFormat sdf = null;
	
	private long totalSize;
	
	public void setup(Context context) {
		log.info("****** mongoDb Reduce  setup ******");
		try {

		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		try {
			log.info("****** mongoDb Reduce reduce ******");
			log.info(">>>>>> mongoDb Reduce key >>>>>>>>>>>>>>>>>>>>>>>>>>"+key);
			totalSize = totalSize + 1;
//			context.write(new Text(key), new Text("1"));
		} catch (Exception e) {
			log.info("reduce error"+e.getMessage());
			log.error(key, e);
		}
	}

	public void cleanup(Context context) {
		try {
			log.info("****************** mongoDb Reduce  cleanup ***********************");
			log.info(">>>>>> totalSize:"+totalSize);
			long start_time = Long.valueOf(context.getConfiguration().get("start_time"));
			long end_time = System.currentTimeMillis();
			long total_time = (end_time-start_time)/1000;
			context.write(new Text("record_date"), new Text(context.getConfiguration().get("record_date")));
			context.write(new Text("delete count"), new Text(String.valueOf(totalSize)));
			context.write(new Text("time cost"), new Text(String.valueOf(total_time)+ " sec"));
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

}
