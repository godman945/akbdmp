package com.pchome.hadoopdmp.mapreduce.job.dedupip;

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

import com.hadoop.mapreduce.LzoTextInputFormat;

public class JavaDedupIpJob {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private static String SYMBOL = String.valueOf(new char[] { 9, 31 });
		private Text keyOut = new Text();
		private Text valueOut = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] values = value.toString().split(SYMBOL);
			
			String result = values[3];
			
			keyOut.set(result);
			context.write(keyOut, valueOut);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text keyOut = new Text();
		private Text valueOut = new Text();
		int i=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			i=i+1;
			
			String data = key.toString();

			keyOut.set(Integer.toString(i)+"\t"+data);
			context.write(keyOut, valueOut);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Dedup");
		job.setJarByClass(JavaDedupIpJob.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		/* 輸入資料的HDFS路徑 */
		FileInputFormat.addInputPath(job, new Path("/home/webuser/dmp/testData/category"));
		/* 輸出資料的HDFS路徑 */
		FileOutputFormat.setOutputPath(job, new Path("/home/webuser/bessie/output/dedupIp.txt"));

		job.waitForCompletion(true);
	}

}
