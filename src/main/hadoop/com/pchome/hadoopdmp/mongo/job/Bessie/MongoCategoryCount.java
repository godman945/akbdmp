package com.pchome.hadoopdmp.mongo.job.Bessie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoCategoryCount {

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.pcb_area");
//		conf.set("mongo.input.query",  "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
//		conf.set("mongo.input.query",  "{'update_date':{'$gt':'2017-04-01 23:59:59','$lt':'2017-06-30 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		
		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "Mongo0622");
		Path out = new Path("/home/webuser/bessie/mongo0622.txt");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(MongoCategoryCount.class);
		job.setMapperClass(MongoCategoryCountMapper.class);
		job.setReducerClass(MongoCategoryCountReducer.class);

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