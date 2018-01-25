package com.pchome.hadoopdmp.mapreduce.job.categorylog;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class CategoryLogDriver {

	private static Log log = LogFactory.getLog("CategoryLogDriver");

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

	@Value("${adLog.class.path}")
	private String adLogClassPpath;
	
	@Value("${akb.path.alllog}")
	private String akbPathAllLog;
	
	private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public void drive(String env,String timeType) throws Exception {

		String alllog = analyzerPathAlllog;

		log.info("alllog " + alllog);

		if (StringUtils.isBlank(alllog)) {
			return;
		}

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

		Date date = new Date();
		conf.set("job.date",sdf1.format(date));
		System.out.println("job.date: " + sdf1.format(date));
		log.info("job.date: " + sdf1.format(date));
		
//		conf.set("job.date", (dateStr.matches("\\d{4}-\\d{2}-\\d{2}") || dateStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}")) ? dateStr.substring(0, 10) : "");
//		System.out.println("job.date: " + dateStr.substring(0, 10));
//		log.info("job.date: " + dateStr.substring(0, 10));

		
		
		
		
		// file system
		FileSystem fs = FileSystem.get(conf);

		// date format
		// SimpleDateFormat hdf = new SimpleDateFormat("yyyy-MM-dd" +
		// Path.SEPARATOR + "HH");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String alllogStr = null;
		Path alllogDir = null;
		Path alllogPath = null;

		// job
		log.info("----job start----");

		Job job = new Job(conf, "dmp_Category_Log " + sdf.format(new Date()));
		job.setJarByClass(CategoryLogDriver.class);
		job.setMapperClass(CategoryLogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CategoryLogReducer.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		
		// job.setOutputFormatClass(NullOutputFormat.class);
		if (timeType.equals("day")) {
			FileOutputFormat.setOutputPath(job, new Path(adLogClassPpath + sdf2.format(date)));
			StringBuffer alllogOpRange = new StringBuffer();
			boolean testFlag = Boolean.parseBoolean(inputPathTestingFlag);
			log.info("testFlag=" + testFlag);
			if (testFlag) {
				// testData:
				alllogOpRange.append(inputPathTestingPath);
			} else {
				alllogOpRange.append(analyzerPathAlllog);
				alllogOpRange.append(sdf1.format(date));
			}
			String bessieTempPath = analyzerPathAlllog+sdf1.format(date);
			FileInputFormat.addInputPaths(job, bessieTempPath);
			log.info("file Input Path : " + alllogOpRange);
		} else if (timeType.equals("hour")) {
			StringBuffer alllogOpRange = new StringBuffer();
			alllogOpRange.append(analyzerPathAlllog);
			alllogOpRange.append(sdf1.format(date));
//			/home/webuser/akb/storedata/alllog/2017-10-01/00
			String timePath  = "";
			Calendar calendar = Calendar.getInstance();  
			if(calendar.get(Calendar.HOUR_OF_DAY) == 24){
				calendar.add(Calendar.DAY_OF_MONTH, -1); 
				timePath = sdf1.format(calendar.getTime())+"/00";
			}else{
				if(calendar.get(Calendar.HOUR_OF_DAY) <= 10){
					timePath = sdf1.format(calendar.getTime()) +"/"+ "0"+(calendar.get(Calendar.HOUR_OF_DAY) - 1);
				}else{
					timePath = sdf1.format(calendar.getTime()) +"/"+ (calendar.get(Calendar.HOUR_OF_DAY) - 1);	
				}
			}
			String bessieTempPath = akbPathAllLog + timePath;
//			FileOutputFormat.setOutputPath(job, new Path(adLogClassPpath +"/"+ timePath));
//			FileInputFormat.addInputPaths(job, bessieTempPath);
			
			FileOutputFormat.setOutputPath(job, new Path("/home/webuser/alex/"+sdf2.format(date)));
			FileInputFormat.addInputPaths(job, "/home/webuser/akb/storedata/alllog/2018-01-25/00");
			log.info("file Input Path : " + alllogOpRange);
			
//			alllogStr = alllog + dateStr.replaceAll(" ", "/");
//			alllogDir = new Path(alllogStr);
//			if (fs.exists(alllogDir)) {
//				alllogPath = alllogDir;
//				FileInputFormat.addInputPath(job, alllogPath);
//				log.info(alllogDir.toString() + " is exists");
//			} else {
//				log.info(alllogDir.toString() + " is not exists");
//				return;
//			}

		} else {
			log.info("date = null");
			return;
		}

		log.info("alllogPath=" + alllogPath);

		//load jar path
		String[] jarPaths = {
				"/home/webuser/dmp/webapps/analyzer/lib/commons-lang-2.6.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/commons-logging-1.1.1.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/log4j-1.2.15.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/mongo-java-driver-2.11.3.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/softdepot-1.0.9.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/solr-solrj-4.5.0.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/noggit-0.5.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/httpcore-4.2.2.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/httpclient-4.2.3.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/httpmime-4.2.3.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/mysql-connector-java-5.1.12-bin.jar",

				// add kafka jar
				"/home/webuser/dmp/webapps/analyzer/lib/kafka-clients-0.9.0.0.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/kafka_2.11-0.9.0.0.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/slf4j-api-1.7.19.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/slf4j-log4j12-1.7.6.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/json-smart-2.3.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/asm-1.0.2.jar" 
		}; 
		for (String jarPath : jarPaths) {
			DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
		}

		//load 分類表、分類個資表、log4j檔
		String[] filePaths = {
				hdfsPath + "/home/webuser/dmp/crawlBreadCrumb/data/pfp_ad_category_new.csv",
				hdfsPath + "/home/webuser/dmp/readingdata/ClsfyGndAgeCrspTable.txt",
				hdfsPath + "/home/webuser/dmp/alex/log4j.xml"
		};
		for (String filePath : filePaths) {
			DistributedCache.addCacheFile(new URI(filePath), job.getConfiguration());
		}

		// //delete old doc for specific date
		// deleteMongoOldDoc(dateStr.substring(0, 10));

		if (job.waitForCompletion(true)) {
			log.info("Job is OK");
		} else {
			log.info("Job is Failed");
		}

	}

	public static void printUsage() {
		System.out.println("Usage(hour): [DATE] [HOUR]");
		System.out.println("Usage(day): [DATE]");
		System.out.println();
		System.out.println("[DATE] format: yyyy-MM-dd");
	}

	public static void main(String[] args) throws Exception {
		log.info("====driver start====");
		String date = "";
		boolean jobFlag = false;
		if(args.length != 2){
			jobFlag = true;
		}else if(!args[0].equals("prd") && !args[0].equals("stg")){
			jobFlag = true;
		}else if(!args[1].equals("day") && !args[1].equals("hour")){
			jobFlag = true;
		}
		if(jobFlag){
			printUsage();
			return;
		}
		
		if(args[0].equals("prd")){
			System.setProperty("spring.profiles.active", "prd");
		}else{
			System.setProperty("spring.profiles.active", "stg");
		}
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		CategoryLogDriver CategoryDriver = (CategoryLogDriver) ctx.getBean(CategoryLogDriver.class);
		CategoryDriver.drive(args[0],args[1]);
		log.info("====driver end====");
	}

}
