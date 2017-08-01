package com.pchome.hadoopdmp.mapreduce.job.dedupip;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
@Scope("prototype")
public class DedupIpJob {

	private static Log log = LogFactory.getLog("DedupIpJob");
	
	@Value("${hpd11.fs.default.name}")
	private String hdfsPath;

	@Value("${hpd11.hadoop.job.ugi}")
	private String jobUgi;

	@Value("${hpd11.mapred.job.tracker}")
	private String tracker;

	@Value("${hpd11.mapred.map.output.compression.codec}")
	private String codec;

	@Value("${hpd11.mapred.map.tasks.speculative.execution}")
	private String mapredExecution;

	@Value("${hpd11.mapred.reduce.tasks.speculative.execution}")
	private String mapredReduceExecution;

	@Value("${hpd11.mapred.task.timeout}")
	private String mapredTimeout;

	@Value("${crawlBreadCrumb.urls.path}")
	private String crawlBreadCrumbUrlsPath;

	@Value("${analyzer.path.alllog}")
	private String analyzerPathAlllog;

	@Value("${input.path.testingflag}")
	private String inputPathTestingFlag;

	@Value("${input.path.testingpath}")
	private String inputPathTestingPath;


	public static void main(String[] args) throws Exception {
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd");
		Date current = new Date();
		String date = sdFormat.format(current);
		
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		DedupIpJob dedupIpJob = ctx.getBean(DedupIpJob.class);
		dedupIpJob.drive(date);
	}
	
	public void drive(String date) throws Exception {
	    
		// hdfs
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", jobUgi);
		conf.set("fs.defaultFS", hdfsPath);
		conf.set("mapreduce.jobtracker.address", tracker);
		conf.set("mapreduce.map.output.compress.codec", codec);
		conf.set("mapreduce.map.speculative", mapredExecution);
		conf.set("mapreduce.reduce.speculative", mapredReduceExecution);
		conf.set("mapreduce.task.timeout", mapredTimeout);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3072m");

		Job job = new Job(conf, "dedupIpJob");
		job.setJarByClass(DedupIpJob.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPaths(job, "/home/webuser/dmp/testData/category");
		FileOutputFormat.setOutputPath(job, new Path("/home/webuser/bessie/output/dedupIp.txt"));
		
		job.setNumReduceTasks(1);
		
	}
	

	
	public static class MyMapper extends Mapper<Object, BSONObject, Text, Text> {
		
		private static String SYMBOL = String.valueOf(new char[] { 9, 31 });
		private Text keyOut = new Text();
		private Text valueOut = new Text();

		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			
			String[] values = value.toString().split(SYMBOL);
			String result = values[3];
			
			keyOut.set(result);
			context.write(keyOut, valueOut);
			
		}
	}

	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text keyOut = new Text();
		private Text valueOut = new Text();
		int i=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			i=i+1;
			
			String data = key.toString();

			keyOut.set(Integer.toString(i)+"\t"+data);
			context.write(keyOut, valueOut);
		}
	}

}