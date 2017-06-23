package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MapReduceMongoJob {
	private static Log log = LogFactory.getLog("MapReduceMongoJob");

	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text> {
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			try {
				String update_date = value.get("update_date").toString();
				String category_info_str = value.get("category_info").toString();
				String user_id = value.get("user_id").toString();
				Map<String, Object> user_info = (Map<String, Object>) value.get("user_info");
				List<Map<String, Object>> category_info = (List<Map<String, Object>>) value.get("category_info");
				String userType = user_info.get("type").toString();
				Map<String, Set<String>> allMap = new HashMap<String, Set<String>>();

				String ad_class ="";
				for (Map<String, Object> category : category_info) {
					ad_class = category.get("category").toString();
					String categoryKey = ad_class + "_" + userType.toUpperCase();
					log.info(">>>>>> categoryKey:" + categoryKey);

//					if (!allMap.containsKey(ad_class)) {
//						Set<String> set = new HashSet<>();
//						set.add(user_id);
//						allMap.put(ad_class, set);
//					} else {
//						Set<String> set = allMap.get(ad_class);
//						set.add(user_id);
//					}
					context.write(new Text(categoryKey), new Text());
					
				}

				
				
				// 0015022500000000
				// 0015022720350000
//				String group = "bessie";
				if(ad_class.equals("0015022500000000")){
					context.write(new Text("0015022500000000"), new Text(user_id));
				}
				if(ad_class.equals("0015022720350000")){
					context.write(new Text("0015022720350000"), new Text(user_id));
				}
				
				
//				Set<String> data = new HashSet<>();
//				for (Map.Entry<String, Set<String>> entry : allMap.entrySet()) {
//					if (entry.equals("0015022500000000")) {
//						data.addAll(entry.getValue());
//					}
//					if (entry.equals("0015022720350000")) {
//						data.addAll(entry.getValue());
//					}
//				}
//				context.write(new Text("0015022500000000_0015022720350000_TOTAL"),	new Text(String.valueOf(data.size())));

			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				log.info(">>>>> reduce key: " + key);
				
				
				if (key.equals("0015022500000000")) {
					for (Text text : values) {
						log.info(">>>>>alex 0015022500000000 : " + text);
					}
				}
				if(key.equals("0015022720350000")){
					for (Text text : values) {
						log.info(">>>>>alex 0015022720350000 : " + text);
					}
				}
				
				
				if(key.toString().indexOf("_") > 0){
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					log.info(">>>>> reduce sum: " + sum);
					context.write(key, new Text(String.valueOf(sum)));
				}
			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.class_count");
		// conf.set("mongo.input.query",
		// "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
		// conf.set("mongo.input.query", "{'update_date':{'$gt':'2017-06-19
		// 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);

		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "alex_test");
		Path out = new Path("/home/webuser/alex/mongo");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(MapReduceMongoJob.class);
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