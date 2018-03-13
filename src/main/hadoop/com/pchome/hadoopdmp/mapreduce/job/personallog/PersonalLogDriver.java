package com.pchome.hadoopdmp.mapreduce.job.personallog;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
public class PersonalLogDriver {

	private static Log log = LogFactory.getLog("PersonalLogDriver");

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
	
	private Date date = new Date();
	private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void drive(String env,String timeType) throws Exception {
		log.info(">>>>>> PersonalLog Job Date: " + sdf1.format(date));
		log.info(">>>>>> env: " + env);
		log.info(">>>>>> timeType: " + timeType);
		
		JobConf jobConf = new JobConf();
		jobConf.setNumMapTasks(8);
		jobConf.set("mapred.max.split.size","100388608");
		jobConf.set("mapred.min.split.size","100388608");
		jobConf.set("mapred.child.java.opts", "-Xmx2g");
		jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");
		jobConf.set("mapred.compress.map.output", "true");
		jobConf.set("spring.profiles.active", env);
		jobConf.set("hadoop.job.ugi", jobUgi);
		jobConf.set("fs.defaultFS", hdfsPath);
		jobConf.set("mapreduce.jobtracker.address", tracker);
		jobConf.set("mapreduce.map.output.compress.codec", codec);
		jobConf.set("mapreduce.map.speculative", mapredExecution);
		jobConf.set("mapreduce.reduce.speculative", mapredReduceExecution);
		jobConf.set("mapreduce.task.timeout", mapredTimeout);
		jobConf.set("mapred.child.java.opts", "-Xmx4072m");
		jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4072m");
		jobConf.set("spring.profiles.active", env);
		jobConf.set("job.date",sdf1.format(date));
		
		FileSystem fs = FileSystem.get(jobConf);
		
		//hdfs存在則刪除
		deleteExistedDir(fs, new Path("/home/webuser/dmp/adLogClassStg/personallog/2018-03-12"), true);
		
		// job
		log.info("----job start----");
		Job job = new Job(jobConf, "dmp_personal_log " + sdf.format(date));
		job.setJarByClass(PersonalLogDriver.class);
		job.setMapperClass(PersonalLogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PersonalLogReducer.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
//		job.setMapSpeculativeExecution(false);
		
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
			String timePath  = "";
			Calendar calendar = Calendar.getInstance();  
			if(calendar.get(Calendar.HOUR_OF_DAY) == 24){
				calendar.add(Calendar.DAY_OF_MONTH, -1); 
				timePath = sdf1.format(calendar.getTime())+"/00";
			}else{
				if(calendar.get(Calendar.HOUR_OF_DAY) < 10){
					timePath = sdf1.format(calendar.getTime()) +"/"+ "0"+(calendar.get(Calendar.HOUR_OF_DAY) - 1);
				}else{
					timePath = sdf1.format(calendar.getTime()) +"/"+ (calendar.get(Calendar.HOUR_OF_DAY) - 1);	
				}
			}
			
			//輸入
			String inputPpath = "/home/webuser/akb/storedata/alllog/2018-03-12";//+sdf1.format(date);
			//輸出
			String outputPath = "/home/webuser/dmp/adLogClassStg/personallog/2018-03-12";//+sdf1.format(date);
			log.info(">>>>>>INPUT PATH:"+inputPpath);
			log.info(">>>>>>OUTPUT PATH:"+outputPath);
			
			FileInputFormat.addInputPaths(job, inputPpath);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
		} else {
			log.info("date = null");
			return;
		}

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

		if (job.waitForCompletion(true)) {
			log.info("Job is OK");
		} else {
			log.info("Job is Failed");
		}

	}

	
	 public static boolean deleteExistedDir(FileSystem fs, Path path, boolean recursive) throws IOException {
	        try {
	            // check path exists
	            if (fs.exists(path)) {
	                return fs.delete(path, recursive);
	            }
	            return true;
	        } catch (Exception e) {
	            log.error("Delete " + path + " error: ", e);
	        }
	        return false;
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
		PersonalLogDriver CategoryDriver = (PersonalLogDriver) ctx.getBean(PersonalLogDriver.class);
		CategoryDriver.drive(args[0],args[1]);
		log.info("====driver end====");
	}

}
