package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.springframework.stereotype.Component;

@Component
public class MongoDbMapper extends Mapper<Object, BSONObject, Text, Text> {
	Log log = LogFactory.getLog(this.getClass());
	

	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private Text totalSizeCount = new Text();
	private Date date = new Date();
	public static String record_date;
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();//分類表	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	@Override
	public void setup(Context context) {
		log.info(">>>>>> mongoDb Mapper  setup >>>>>>>>>>>>>>>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
	}
	@Override
	public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
		try {
			totalSizeCount.set(new Text("sizeCount"));
			context.write(new Text(String.valueOf(key+"_count")), new Text(String.valueOf(key)));
			
			int range = 365;
			
//			Date date1 = sdf.parse(String.valueOf(value.get("create_date")));
//			Date date2 = sdf.parse(String.valueOf(value.get("update_date")));
			
			Date date1 = sdf.parse(String.valueOf(value.get("update_date")));
			Date date2 = sdf.parse(sdf.format(date));
			
			Calendar cal1 = Calendar.getInstance();
	        cal1.setTime(date1);
	        
	        Calendar cal2 = Calendar.getInstance();
	        cal2.setTime(date2);
	        int rangeDay = ( int ) ((date2.getTime() - date1.getTime()) / (1000*3600*24 )); 
			if(rangeDay > range){
				log.info(">>>>>> Mapper write key:" + key);
				log.info(">>>>>> rangeDay:" + rangeDay);
//				log.info(">>>>>> Mapper write value:" + value);
//				log.info(">>>>>> Mapper write count:" + count);
				keyOut.set(new Text(key.toString())+"_delete");
				context.write(keyOut, new Text(value.toString()));
			}
		} catch (Exception e) {
			log.error(">>>>>> " + e.getMessage());
		}

	}

}
