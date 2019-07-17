package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class DmpLogDriver {

	private static Log log = LogFactory.getLog("DmpLogDriver");

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
	
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat sdfHour = new SimpleDateFormat("HH");
	String logInputPath;
	String outPath;
	
	public void drive(String env,String dmpDate,String dmpHour) throws Exception {
		try {
			Calendar dmpDateCalendar = Calendar.getInstance();
			dmpDateCalendar.setTime(sdf.parse(dmpDate));
			
			
			JobConf jobConf = new JobConf();
			jobConf.setNumMapTasks(5);
			jobConf.set("mapred.max.split.size","3045728"); //3045728 49 //3045728000 7
			jobConf.set("mapred.min.split.size","1015544"); //1015544 49 //1015544000 7
			//ask推测执行
			jobConf.set("mapred.map.tasks.speculative.execution","true");
			jobConf.set("mapred.reduce.tasks.speculative.execution","true");
			//JVM
			jobConf.set("mapred.child.java.opts", "-Xmx8192M");
			jobConf.set("mapreduce.map.memory.mb", "8192");
			jobConf.set("mapreduce.reduce.memory.mb", "8192");
			jobConf.set("mapreduce.job.running.map.limit", "100");
			jobConf.set("spring.profiles.active", env);
			jobConf.set("job.date",dmpDate);
			jobConf.set("job.hour",dmpHour);
			
			// hdfs
			Configuration conf = new Configuration();
			conf.set("mapreduce.map.output.compress.codec", codec);
			conf.set("mapreduce.map.speculative", mapredExecution);
			conf.set("mapreduce.reduce.speculative", mapredReduceExecution);
			conf.set("mapreduce.task.timeout", mapredTimeout);
			conf.set("mapred.map.tasks.speculative.execution","true");
			conf.set("mapred.reduce.tasks.speculative.execution","true");
			conf.set("mapred.child.java.opts", "-Xmx8192M");
			conf.set("mapreduce.jobtracker.address", "hpd11.mypchome.com.tw:9001");
			conf.set("mapreduce.map.memory.mb", "8192");
	        conf.set("mapreduce.map.java.opts", "-Xmx8192m");
	        conf.set("mapreduce.reduce.memory.mb", "8192");
	        conf.set("mapreduce.reduce.java.opts", "-Xmx8192m");
	        conf.set("spring.profiles.active", env);
	        conf.set("job.date",dmpDate);
	        conf.set("job.hour",dmpHour);
	        
	        //輸入檔案
	        List<Path> listPath = new ArrayList<Path>();  
	        FileSystem fs = FileSystem.get(conf);
	        
	        
			//載入bu log file
	        Path buPath = new Path("/home/webuser/akb/storedata/bulog/"+dmpDate+"/"+dmpHour);
	        FileStatus[] buStatus = fs.listStatus(buPath); 
			for (FileStatus fileStatus : buStatus) {
				String pathStr = fileStatus.getPath().toString();
				String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
				if(extensionName.equals("LZO")) {
					listPath.add(new Path(fileStatus.getPath().toString()));
				}
			}
			//載入kdcl log file
	        Path kdclPath = new Path("/home/webuser/akb/storedata/alllog/"+dmpDate+"/"+dmpHour);
	        FileStatus[] kdclStatus = fs.listStatus(kdclPath); 
			for (FileStatus fileStatus : kdclStatus) {
				String pathStr = fileStatus.getPath().toString();
				String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
				if(extensionName.equals("LZO")) {
					listPath.add(new Path(fileStatus.getPath().toString()));
				}
			}
			//載入pacl log file
			Path paclPath = new Path("/home/webuser/pa/storedata/alllog/"+dmpDate+"/"+dmpHour);
	        FileStatus[] paclStatus = fs.listStatus(paclPath); 
			for (FileStatus fileStatus : paclStatus) {
				String pathStr = fileStatus.getPath().toString();
				String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
				if(extensionName.equals("LZO")) {
					listPath.add(new Path(fileStatus.getPath().toString()));
				}
			}
			
			
//			listPath.add(new Path("hdfs://hpd11.mypchome.com.tw:9000/home/webuser/akb/storedata/alllog/2019-05-29/16/kdcl1-16.lzo"));
			Path[] paths = new Path[listPath.size()];  
			listPath.toArray(paths);
			for (Path path : paths) {
				log.info(">>>>>>>>>>JOB INPUT PATH:"+path.toString());
			}
			Job job = new Job(jobConf, "dmp_log_"+ env + "_druid_test");
			job.setJarByClass(DmpLogDriver.class);
			job.setMapperClass(DmpLogMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(DmpLogReducer.class);
			job.setInputFormatClass(LzoTextInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.getConfiguration().set("mapreduce.output.basename", "druid_"+dmpDate+"_"+dmpHour);
			job.setNumReduceTasks(1);//1個reduce 
			job.setMapSpeculativeExecution(false);
			if(env.equals("prd")) {
				deleteExistedDir(fs, new Path("/home/webuser/alex/druid/"+dmpDate+"/"+dmpHour), true);
				FileOutputFormat.setOutputPath(job, new Path("/home/webuser/alex/druid/"+dmpDate+"/"+dmpHour));
			}else {
				deleteExistedDir(fs, new Path("/home/webuser/alex/druid/"+dmpDate+"/"+dmpHour), true);
				FileOutputFormat.setOutputPath(job, new Path("/home/webuser/alex/druid/"+dmpDate+"/"+dmpHour));
			}
			log.info("JOB OUTPUT PATH:"+"/home/webuser/alex/druid/"+dmpDate+"/"+dmpHour);
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setCompressOutput(job, true);  //job使用压缩  
	        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);  
		
			
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
	
			String[] filePaths = {
					hdfsPath + "/home/webuser/dmp/crawlBreadCrumb/data/pfp_ad_category_new.csv",
					hdfsPath + "/home/webuser/dmp/readingdata/ClsfyGndAgeCrspTable.txt",
					hdfsPath + "/home/webuser/dmp/alex/log4j.xml",
					hdfsPath + "/home/webuser/dmp/jobfile/DMP_24h_category.csv",
					hdfsPath + "/home/webuser/dmp/jobfile/DMP_Ruten_category.csv",
					hdfsPath + "/home/webuser/dmp/jobfile/GeoLite2-City.mmdb",
					hdfsPath + "/home/webuser/dmp/jobfile/ThirdAdClassTable.txt"
			};
			for (String filePath : filePaths) {
				DistributedCache.addCacheFile(new URI(filePath), job.getConfiguration());
			}
	
			if (job.waitForCompletion(true)) {
				log.info("Job1 is OK");
			} else {
				log.info("Job1 is Failed");
			}
			
			
		 } catch (Exception e) {
			 log.error("drive error>>>>>> "+ e);
	     }
	}

	public static void printUsage() {
		System.out.println("Usage(hour): [stg or prd] [hour]");
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
	
	
	/**
	 * args[0]:env
	 * args[1]:date
	 * args[2]:hour
	 * */
	public static void main(String[] args)  {
		try {
			log.info("====HADOOP JOB START====");
			if(args.length != 3) {
				System.out.println("arg length fail");
			}
			if(args[0].equals("prd")){
				System.setProperty("spring.profiles.active", "prd");	
			}else {
				System.setProperty("spring.profiles.active", "stg");
			}
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			DmpLogDriver dmpLogDriver = (DmpLogDriver) ctx.getBean(DmpLogDriver.class);
			dmpLogDriver.drive(args[0],args[1],args[2]);
			log.info("====HADOOP JOB END====");
		}catch(Exception e) {
			log.error(e.getMessage());
		}
		
		
		
		
		
		
//		if(args[0].equals("prd")){
//			System.setProperty("spring.profiles.active", "prd");
//		}else{
//			if(args.length != 4) {
//				System.setProperty("druid.test", "true");
//				log.info("====stg setup fail====");
//			}
//			System.setProperty("spring.profiles.active", "stg");
//		}
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		DmpLogDriver dmpLogDriver = (DmpLogDriver) ctx.getBean(DmpLogDriver.class);
//		dmpLogDriver.drive(args[0],args[1],args[2],args[3]);
//		log.info("====driver end====");
	}
}
