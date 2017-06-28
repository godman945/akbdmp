package com.pchome.hadoopdmp.mongo.job.Bessie;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoCategoryCountMapper extends Mapper<Object, BSONObject, Text, Text> {

	public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
		
		try{
			String user_id = value.get("user_id").toString();
	
			String update_date = value.get("update_date").toString();
	
			String category_info_str = value.get("category_info").toString();
	
			// List<Map<String, Object>> category_info_str = (List<Map<String,
			// Object>>) value.get("category_info");
	
			Map<String, Object> user_info = (Map<String, Object>) value.get("user_info");
	
			Gson gson = new Gson();
			TypeToken<List<Map<String, Object>>> token = new TypeToken<List<Map<String, Object>>>() {
			};
			List<Map<String, Object>> personList = gson.fromJson(category_info_str, token.getType());
	
			String category = "";
			for (Map<String, Object> map : personList) {
				category = (String) map.get("category");
				System.out.println("category : " + category);
	
			}
	
			// ObjectMapper mapper = new ObjectMapper();
			// ReadMongoBean user = mapper.readValue(category_info_str,
			// ReadMongoBean.class);
			// System.out.println("user Bean: "+user.getCategory_info().size());
			//
			// String category="";
			// for (Map<String, Object> map : user.getCategory_info()) {
			// category=(String) map.get("category");
			// System.out.println("category : "+category);
			//
			// }
			System.out.println("category_info_str : " + category);
			
			System.out.println(user_id);
	
			context.write(new Text(user_id), new Text(update_date));
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}

}
