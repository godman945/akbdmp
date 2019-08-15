package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		
		FileSystem fs = FileSystem.get(conf);
		System.out.println("1111111111");
		fs.exists(new Path("/durid_source/word_count/a.txt"));
		fs.exists(new Path("hdfs://druid1.mypchome.com.tw:9000/druid_source/word_count/a.txt"));
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/durid_source/word_count"));
		FileOutputFormat.setOutputPath(job, new Path("/durid_source"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
