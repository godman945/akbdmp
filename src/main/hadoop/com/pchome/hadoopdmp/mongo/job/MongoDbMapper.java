package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.springframework.stereotype.Component;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

@Component
public class MongoDbMapper extends Mapper<Object, BSONObject, Text, Text> {
	Log log = LogFactory.getLog(this.getClass());
	private Text keyOut = new Text();
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();//分類表	
	private DBCollection dBCollection;
	@SuppressWarnings("deprecation")
	@Override
	public void setup(Context context) {
		log.info(">>>>>> mongoDb Mapper  setup >>>>>>>>>>>>>>>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			Mongo mongo;
			mongo = new Mongo("mongodb.mypchome.com.tw");
			DB db = mongo.getDB("dmp");
			db.authenticate("webuser", "MonG0Dmp".toCharArray());  
			this.dBCollection = db.getCollection("user_detail");
			
//			Mongo mongo;
//			mongo = new Mongo("192.168.1.37",27017);
//			DB db = mongo.getDB("dmp");
//			db.authenticate("webuser", "axw2mP1i".toCharArray());
//			this.dBCollection = db.getCollection("user_detail");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}  
		
	}
	
	@Override
	public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
		try {
			log.info(">>>>>> Mapper write key:" + key);
//			log.info(">>>>>> Mapper write value:" + value);
			DBObject object = (DBObject)value;
			String dbKey = key.toString();
			this.dBCollection.remove(object);
			keyOut.set(new Text(dbKey));
			context.write(keyOut, new Text(value.toString()));
		} catch (Exception e) {
			log.error(">>>>>> " + e.getMessage());
		}

	}

}
