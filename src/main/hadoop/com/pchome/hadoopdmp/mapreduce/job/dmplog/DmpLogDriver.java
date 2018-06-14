package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
	
	private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public void drive(String env,String timeType) throws Exception {
		try {
			Calendar calendar = Calendar.getInstance();
			
			
			
			JobConf jobConf = new JobConf();
//			jobConf.setNumMapTasks(8);
			//4.78mb
			jobConf.set("mapred.max.split.size","3045728"); //6045728-11個map、5個reduce
			jobConf.set("mapred.min.split.size","1015544"); //2015544-11個map、5個reduce
			
			//ask推测执行
			jobConf.set("mapred.map.tasks.speculative.execution","true");
			jobConf.set("mapred.reduce.tasks.speculative.execution","true");
			
			
			
			//指定DataNode用於balancer的帶寬
//			jobConf.set("dfs.datanode.balance.bandwidthPerSec", "1048576");
//			//默认是1，表示一个JVM上最多可以顺序执行的task数目
//			jobConf.set("mapred.job.reuse.jvm.num.tasks", "10");
//			//工作線程池用來處理客户端的遠程過程調用及集羣守護進程的調用
//			jobConf.set("dfs.namenode.handler.count", "80");
//			jobConf.set("dfs.datanode.max.transfer.threads", "8192");
			
			//配置每个map或reduce使用的内存数量，默认是200m，一般情况下，该值设置为 总内存/并发数量(=核数)
			
//			jobConf.set("yarn.app.mapreduce.am.command-opts","Xmx5734m");

			
//			jobConf.set("mapred.child.java.opts", "-Xmx2g");
//			jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");
//			jobConf.set("mapred.compress.map.output", "true");
			jobConf.set("spring.profiles.active", env);
			
			// hdfs
			Configuration conf = new Configuration();
			conf.set("hadoop.job.ugi", jobUgi);
			conf.set("fs.defaultFS", hdfsPath);
			//hadoop叢集位置
			conf.set("mapreduce.jobtracker.address", tracker);
			//com.hadoop.compression.lzo.LzoCodec
			conf.set("mapreduce.map.output.compress.codec", codec);
			conf.set("mapreduce.map.speculative", mapredExecution);
			conf.set("mapreduce.reduce.speculative", mapredReduceExecution);
			conf.set("mapreduce.task.timeout", mapredTimeout);
			
			
			conf.set("mapred.map.tasks.speculative.execution","true");
			conf.set("mapred.reduce.tasks.speculative.execution","true");
			
//			conf.set("dfs.datanode.balance.bandwidthPerSec", "1048576");
//			conf.set("mapred.job.reuse.jvm.num.tasks", "10");
//			conf.set("dfs.namenode.handler.count", "80");
//			conf.set("dfs.datanode.max.transfer.threads", "8192");
//			conf.set("mapred.child.java.opts", "-Xmx4072m");
//			conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4072m");
//			conf.set("mapred.max.split.size","128388608");
//			conf.set("mapred.min.split.size","128388608");
//			conf.set("mapreduce.min.split.size","128388608");
//			conf.set("mapreduce.max.split.size","128388608");
//			conf.set("dfs.namenode.fs-limits.min-block-size","1048576");
//			conf.set("dfs.namenode.fs-limits.max-blocks-per-file","1048576");
			
			
			
			
			if(calendar.get(Calendar.HOUR_OF_DAY) == 0){
				calendar.add(Calendar.DAY_OF_MONTH, -1);
				conf.set("job.date",sdf1.format(calendar.getTime()));
				jobConf.set("job.date",sdf1.format(calendar.getTime()));
			}else{
				conf.set("job.date",sdf1.format(calendar.getTime()));
				jobConf.set("job.date",sdf1.format(calendar.getTime()));
			}
			// file system
			conf.set("spring.profiles.active", env);
			FileSystem fs = FileSystem.get(conf);
	
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = new Date();
			// job
			log.info("----job start----");
	
			Job job = new Job(jobConf, "dmp_log_"+ env + "_" + sdf.format(date));
			job.setJarByClass(DmpLogDriver.class);
			job.setMapperClass(DmpLogMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(DmpLogReducer.class);
			job.setInputFormatClass(LzoTextInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);//5個reduce
			job.setMapSpeculativeExecution(false);
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
				
				String tempPath = analyzerPathAlllog+sdf1.format(date);
				FileInputFormat.addInputPaths(job, tempPath);
				log.info("file Input Path : " + alllogOpRange);
				
			} else if (timeType.equals("hour")) {
				//測試機前4小時版本
				String timePath  = "";
				Calendar calendar2 = Calendar.getInstance();
				if( (calendar2.get(calendar2.HOUR_OF_DAY) >= 0) &&  (calendar2.get(calendar2.HOUR_OF_DAY) <= 3) ){
					calendar2.add(calendar2.DAY_OF_MONTH, -1); 
					
					if (calendar2.get(calendar2.HOUR_OF_DAY) == 0){
						timePath = sdf1.format(calendar2.getTime())+"/20";
					}
					
					if (calendar2.get(calendar2.HOUR_OF_DAY) == 1){
						timePath = sdf1.format(calendar2.getTime())+"/21";
					}
					
					if (calendar2.get(calendar2.HOUR_OF_DAY) == 2){
						timePath = sdf1.format(calendar2.getTime())+"/22";
					}
					
					if (calendar2.get(calendar2.HOUR_OF_DAY) == 3){
						timePath = sdf1.format(calendar2.getTime())+"/23";
					}
				}else {
					if(String.valueOf(calendar2.get(calendar2.HOUR_OF_DAY) - 4).length() < 2){
						timePath = sdf1.format(calendar2.getTime()) +"/"+ "0"+(calendar.get(calendar2.HOUR_OF_DAY) - 4);
					}else{
						timePath = sdf1.format(calendar2.getTime()) +"/"+ (calendar.get(calendar2.HOUR_OF_DAY) - 4);
					}
				}
				//測試機前4小時版本
				
				
				
				
				
//				//正式機前1小時版本
//				String timePath  = "";
//				Calendar calendar2 = Calendar.getInstance();
//				if(calendar2.get(calendar2.HOUR_OF_DAY) == 0){
//					calendar2.add(calendar2.DAY_OF_MONTH, -1); 
//					timePath = sdf1.format(calendar2.getTime())+"/23";
//				}else {
//					if(String.valueOf(calendar2.get(calendar2.HOUR_OF_DAY) - 1).length() < 2){
//						timePath = sdf1.format(calendar2.getTime()) +"/"+ "0"+(calendar.get(calendar2.HOUR_OF_DAY) - 1);
//					}else{
//						timePath = sdf1.format(calendar2.getTime()) +"/"+ (calendar.get(calendar2.HOUR_OF_DAY) - 1);
//					}
//				}
//				//正式機前1小時版本
				
				
				
				//輸入
//				String logInputPath = "/home/webuser/dmp/testData/category/tmp/ruten";	//自己做測試資料			  		
//				String logInputPath = "/home/webuser/dmp/testData/category";				//測試資料(有ruten、24h的資料)
//				String logInputPath = "/home/webuser/analyzer/storedata/alllog/2018-05-22";			//測試path
				String logInputPath = "/home/webuser/akb/storedata/alllog/2018-05-22/06/tmp";			//11點200萬筆資料
//				String logInputPath = "/home/webuser/dmp/testData/category/20180522";			//測試path整天log
//				String logInputPath = akbPathAllLog + timePath; //正式path  /home/webuser/akb/storedata/alllog/2018-05-15/05    	//正式path
				
//				String logInputPath = "/home/webuser/dmp/testData/category/20180522";
				
				//輸出
//				String outputTempPath = "/home/webuser/bessie/output/category";			//測試資料(有ruten、24h的資料)
				String outputTempPath = "/home/webuser/bessie/output/06";
				//hdfs存在則刪除
				deleteExistedDir(fs, new Path(outputTempPath), true);
				
				log.info(">>>>>>INPUT PATH:"+logInputPath);
				log.info(">>>>>>OUTPUT PATH:"+outputTempPath);
				
				FileInputFormat.addInputPaths(job, logInputPath);
				FileOutputFormat.setOutputPath(job, new Path(outputTempPath));
			} else {
				log.info("date = null");
				return;
			}
	
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
					hdfsPath + "/home/webuser/dmp/jobfile/GeoLite2-City.mmdb"
			};
			for (String filePath : filePaths) {
				DistributedCache.addCacheFile(new URI(filePath), job.getConfiguration());
			}
	
			if (job.waitForCompletion(true)) {
				log.info("Job is OK");
			} else {
				log.info("Job is Failed");
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
	
	

	public static void main(String[] args) throws Exception {
//		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
//		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
//		String timePath  = "";
//		Calendar calendar = Calendar.getInstance();
//		System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
//		if(calendar.get(Calendar.HOUR_OF_DAY) == 0){
//			calendar.add(Calendar.DAY_OF_MONTH, -1); 
//			timePath = sdf1.format(calendar.getTime())+"/23";
//		}else {
//			if(String.valueOf(calendar.get(Calendar.HOUR_OF_DAY) - 1).length() < 2){
//				timePath = sdf1.format(calendar.getTime()) +"/"+ "0"+(calendar.get(Calendar.HOUR_OF_DAY) - 1);
//			}else{
//				timePath = sdf1.format(calendar.getTime()) +"/"+ (calendar.get(Calendar.HOUR_OF_DAY) - 1);
//			}
//		}
//		System.out.println(timePath);
		
		log.info("====driver start====");
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
		DmpLogDriver dmpLogDriver = (DmpLogDriver) ctx.getBean(DmpLogDriver.class);
		dmpLogDriver.drive(args[0],args[1]);
		log.info("====driver end====");
	}
//
}
