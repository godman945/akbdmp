package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class ImportWeblogsFromMongo {

	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text> {
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
//			System.out.println("Key: " + key);
//			System.out.println("Value: " + value);
//			String uuid = value.get("uuid").toString();
//			String behavior = value.get("behavior").toString();
//			String record_date = value.get("record_date").toString();
			
			String uuid = value.get("url").toString();
			
//			String date = value.get("date").toString();
//			String time = value.get("time").toString();
//			String ip = value.get("ip").toString();
			
			
//			String output = behavior+"     "+record_date;
			
			context.write(new Text(uuid), new Text(uuid));
		}
	}
	
	
	
	
	

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf,  "mongodb://192.168.1.37:27017/pcbappdev.class_url");
//		MongoConfigUtil.setInputURI(conf, "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.class_count");s
		MongoConfigUtil.setCreateInputSplits(conf, false);
		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "Mongo0622");
		Path out = new Path("/home/webuser/bessie/mongo0622.txt");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(ImportWeblogsFromMongo.class);
		job.setMapperClass(ReadWeblogsFromMongo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}