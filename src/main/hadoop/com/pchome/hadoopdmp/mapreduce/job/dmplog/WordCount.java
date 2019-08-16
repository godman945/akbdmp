package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("hdfs://druid1.mypchome.com.tw:9000/durid_source"), true);
		System.out.println("1111111111");
		System.out.println(fs.exists(new Path("/durid_source/word_count/a.txt")));
		System.out.println(fs.exists(new Path("hdfs://druid1.mypchome.com.tw:9000/druid_source/kdcl_log/2019-08-04/00/20190804_00_4c.log.lzo")));
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setCombinerClass(IntSumReducer.class);

		
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(IntWritable.class);
		
		
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://druid1.mypchome.com.tw:9000/druid_source/kdcl_log/2019-08-04/00/20190804_00_4c.log.lzo"));
		FileOutputFormat.setOutputPath(job, new Path("/durid_source"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
