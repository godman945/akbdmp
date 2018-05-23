package com.pchome.hadoopdmp.mapreduce.job.dedupip;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class JavaDedupIpJob {
	
	static Log log = LogFactory.getLog("JavaDedupIpJob");

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private static String SYMBOL = String.valueOf(new char[] { 9, 31 });
		private Text keyOut = new Text();
		private Text valueOut = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			log.info(">>>>>> JavaDedupIpJob Mapper  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
			log.info(">>>>>> value : "+value);
			
			String[] values = value.toString().split(SYMBOL);
			
			String result = values[3];
			
			keyOut.set(result);
			context.write(keyOut, valueOut);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text keyOut = new Text();
		int i=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			log.info(">>>>>> JavaDedupIpJob reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
			log.info(">>>>>> key : "+key);
			
			i=i+1;
			
			String data = key.toString();

			keyOut.set(Integer.toString(i)+"\t"+data);
			context.write(keyOut,new Text(""));
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
		FileInputFormat.addInputPath(job, new Path("/home/webuser/akb/storedata/alllog/2018-04-16/06"));//home/webuser/dmp/testData/category為測試資料
		/* 輸出資料的HDFS路徑 */
		FileOutputFormat.setOutputPath(job, new Path("/home/webuser/bessie/output/new/dedupIp0501.txt"));

		job.waitForCompletion(true);
	}

}
