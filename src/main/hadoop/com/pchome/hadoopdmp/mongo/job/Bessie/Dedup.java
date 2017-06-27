package com.pchome.hadoopdmp.mongo.job.Bessie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Dedup {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private static Text line=new Text();
		private static Text outMap =new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			line = value;
			context.write(line, outMap);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private static Text outReduce=new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			context.write(key,outReduce);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Dedup");
		job.setJarByClass(Dedup.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		/* 輸入資料的HDFS路徑 */
		FileInputFormat.addInputPath(job, new Path("/home/webuser/bessie/Dedup.txt"));
		/* 輸出資料的HDFS路徑 */
		FileOutputFormat.setOutputPath(job, new Path("/home/webuser/bessie/output"));

		job.waitForCompletion(true);
	}

}