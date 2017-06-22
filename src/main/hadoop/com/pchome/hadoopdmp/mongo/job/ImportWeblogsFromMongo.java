package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class ImportWeblogsFromMongo {

	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text> {
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			// System.out.println("Key: " + key);
			// System.out.println("Value: " + value);
			// String uuid = value.get("uuid").toString();
			// String behavior = value.get("behavior").toString();
			// String record_date = value.get("record_date").toString();

			String user_id = value.get("user_id").toString();
			String category_info_str = value.get("category_info").toString();
			
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
			System.out.println("category_info_str : "+category_info_str);
			
			// String date = value.get("date").toString();
			// String time = value.get("time").toString();
			// String ip = value.get("ip").toString();

			// String output = behavior+" "+record_date;

			context.write(new Text(user_id), new Text(category_info_str));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {

			
			context.write(key, values);
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.pcb_area");
		// MongoConfigUtil.setInputURI(conf,
		// "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.class_count");s
		MongoConfigUtil.setCreateInputSplits(conf, false);
		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "Mongo0622");
		Path out = new Path("/home/webuser/bessie/mongo0622.txt");
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