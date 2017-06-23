package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MapReduceMongoJob {
private static Log log = LogFactory.getLog("MapReduceMongoJob");
	
	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text> {
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			// System.out.println("Key: " + key);
			// System.out.println("Value: " + value);
			// String uuid = value.get("uuid").toString();
			// String behavior = value.get("behavior").toString();
			// String record_date = value.get("record_date").toString();
			
			String user_id = value.get("user_id").toString();
			
			
			
			
			System.out.println(user_id);
			
			
			
			
//			String type = value.get("type").toString();
			String update_date = value.get("update_date").toString();
			String category_info_str = value.get("category_info").toString();
			
			Gson gson = new Gson();
			TypeToken<List<Map<String, Object>>> token = new TypeToken<List<Map<String, Object>>>(){};
			List<Map<String, Object>> personList = gson.fromJson(category_info_str, token.getType());
			
			
			log.info(">>>>>> mapper value : "+value);
			
			log.info(">>>>>> mapper user_id : "+user_id);
//			log.info(">>>>>> mapper type : "+type);
			log.info(">>>>>> mapper update_date : "+update_date);
			log.info(">>>>>> mapper category_info_str : "+category_info_str);
			
			
			
			
			
			String category = "";
//			String user_adclass = user_id+"_"+type;
			for (Map<String, Object> map : personList) {
				category = (String) map.get("category");
//				log.info(">>>>>> mapper category : "+category);	
				
//				String adclass_user =  type + "_" + category;
//				
//				context.write(new Text(adclass_user), new Text("1"));
			}
			
			context.write(new Text("TEST"), new Text("1"));
			
//			log.info(">>>>> mapper category_info_str : "+category);
////			context.write(new Text(user_id), new Text(update_date));
//			context.write(new Text("total_user"), new Text("1"));
			
			
//			ObjectMapper mapper = new ObjectMapper();
//			ReadMongoBean user = mapper.readValue(category_info_str, ReadMongoBean.class);
//			System.out.println("user Bean: "+user.getCategory_info().size());
//				
//			String category="";
//			for (Map<String, Object> map : user.getCategory_info()) {
//				category=(String) map.get("category");
//				System.out.println("category : "+category);
//				
//			}
			
			
			// String date = value.get("date").toString();
			// String time = value.get("time").toString();
			// String ip = value.get("ip").toString();
			// String output = behavior+" "+record_date;

			
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			log.info(">>>>> reduce key: "+key);
			log.info(">>>>> reduce values: "+values);
			int sum = 0;
			
			for (Text text : values) {
				sum = sum + 1;
				log.info(">>>>> reduce sum: "+sum);
				
			}
			
			context.write(key,new Text(String.valueOf(sum)));
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.class_count");
//		conf.set("mongo.input.query",  "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
		conf.set("mongo.input.query",  "{'update_date':{'$gt':'2017-06-19 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		
		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "alex_test");
		Path out = new Path("/home/webuser/alex/mongo");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(ImportWeblogsFromMongo.class);
		job.setMapperClass(ReadWeblogsFromMongo.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}