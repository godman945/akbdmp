package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.input.MongoInputSplit;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class MongoDbDriver {

	private static Log log = LogFactory.getLog("MongoDbDriver");

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
	
	public void drive() throws Exception {
////		 Configuration conf = new Configuration();
//		 JobConf conf = new JobConf();
//		 Job job = new Job(conf, "alex_mongo_db_log");
//		 
//		 MongoConfigUtil.setInputURI(conf,"mongodb://webuser:MonG0Dmp@mongodb.mypchome.com.tw/dmp.user_detail");
//		 MongoConfigUtil.setCreateInputSplits(conf, false);
//		 MongoConfigUtil.setReadSplitsFromSecondary(conf, true);
//		 MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
////		 conf.set("mapred.job.tracker", "5");
////		 conf.setNumMapTasks(5);
////		 conf.set("mapred.max.split.size","5000");
////		 conf.set("mapred.min.split.size","5000");
////		 conf.set("mapred.max.split.size","5000");
////		 conf.set("mapred.min.split.size","5000");
////		 conf.set("dfs.namenode.fs-limits.min-block-size","5000");
////		 conf.set("dfs.namenode.fs-limits.max-blocks-per-file","5000");
//		 
//		 job.setJarByClass(MongoDbDriver.class);
//		 job.setMapperClass(MongoDbMapper.class);
//		 job.setReducerClass(MongoDbReducer.class);
//		 job.setMapOutputKeyClass(Text.class);
//		 job.setMapOutputValueClass(Text.class);
//		 job.setOutputKeyClass(Text.class);
//		 job.setOutputValueClass(Text.class);
//		 job.setInputFormatClass(MongoInputFormat.class);
//		 job.setOutputFormatClass(TextOutputFormat.class);
//		 job.setNumReduceTasks(1);
//		 
//		 FileSystem fs = FileSystem.get(conf);
//		 deleteExistedDir(fs, new Path("/home/webuser/dmp/alex/mongo"), true);
//		 Path out = new Path("/home/webuser/dmp/alex/mongo");
//		 FileOutputFormat.setOutputPath(job, out);
//		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
	    
		JobConf jobConf = new JobConf();
		jobConf.set("spring.profiles.active", "stg");
//		jobConf.set("mapred.child.java.opts", "-Xmx2g");
//		jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");
//		jobConf.set("mapred.compress.map.output", "true");
//		jobConf.set("mapred.max.split.size","10000"); //no
//		jobConf.set("mapreduce.input.fileinputformat.split.maxsize", "10000"); //no
//		jobConf.set("mongo.input.split_size", "20000");//no
//		jobConf.setNumTasksToExecutePerJvm(5);
//		jobConf.set("mapred.reduce.tasks", "5");//no
		
//		jobConf.set("mapred.max.split.size","5000");
//		jobConf.set("mapred.min.split.size","5000");
//		jobConf.set("mapreduce.min.split.size","5000");
//		jobConf.set("mapreduce.max.split.size","5000");
//		jobConf.setNumMapTasks(2); //no
//		jobConf.setMaxMapTaskFailuresPercent(2);//no
//		jobConf.setMemoryForMapTask(100);//no
//		jobConf.setNumTasksToExecutePerJvm(2);//no
		
//		jobConf.set("mapred.job.tracker", "5");//no
//		jobConf.setNumMapTasks(2);//no
//		jobConf.set("mapred.max.split.size","5000");//no
//		jobConf.set("mapred.min.split.size","5000");//no
//		jobConf.set("mapred.max.split.size","5000");//no
//		jobConf.set("mapred.min.split.size","5000");//no
//		jobConf.set("dfs.namenode.fs-limits.min-block-size","5000");//no
//		jobConf.set("dfs.namenode.fs-limits.max-blocks-per-file","5000");//no
//		jobConf.set("mapred.child.java.opts", "-Xmx2g");//no
//		jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");//no
//		jobConf.set("mapred.compress.map.output", "true");//no
//		jobConf.set("spring.profiles.active", "stg");//no
//		jobConf.set("hadoop.job.ugi", jobUgi);//no
//		jobConf.set("fs.defaultFS", hdfsPath);//no
//		jobConf.set("mapreduce.jobtracker.address", tracker);//no
//		jobConf.set("mapreduce.map.output.compress.codec", codec);//no
//		jobConf.set("mapreduce.map.speculative", mapredExecution);//no
//		jobConf.set("mapreduce.reduce.speculative", mapredReduceExecution);//no
//		jobConf.set("mapreduce.task.timeout", mapredTimeout);//no
//		jobConf.set("mapred.child.java.opts", "-Xmx4072m");//no
//		jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4072m");//no
//		jobConf.set("mapreduce.min.split.size","128388608");//no
//		jobConf.set("mapreduce.max.split.size","128388608");//no
//		jobConf.set("dfs.namenode.fs-limits.min-block-size","3088608");//no
//		jobConf.set("dfs.namenode.fs-limits.max-blocks-per-file","3088608");//no
//		jobConf.set("mapreduce.input.fileinputformat.split.maxsize","388608");//no
//		jobConf.set("mapreduce.input.fileinputformat.split.minsize","3088608");//no
		
		
		MongoConfigUtil.setInputURI(jobConf,"mongodb://webuser:MonG0Dmp@mongodb.mypchome.com.tw/dmp.user_detail");
		MongoConfigUtil.setInputFormat(jobConf, MongoInputFormat.class);
//		MongoConfigUtil.setCreateInputSplits(jobConf, false);
//		MongoConfigUtil.setSplitSize(jobConf, 10000);
		MongoConfigUtil.setMapper(jobConf, MongoDbMapper.class);
		
//		MongoConfigUtil.setCreateInputSplits(jobConf, false);
//		MongoConfigUtil.setReadSplitsFromShards(jobConf,true);
//		MongoConfigUtil.setSplitSize(jobConf, 9000);
//		MongoConfigUtil.setBSONOutputBuildSplits(jobConf, true);
//		MongoConfigUtil.setLimit(conf, limit);
//		MongoConfigUtil.BSON_READ_SPLITS
//		MongoConfigUtil.setReadSplitsFromSecondary(jobConf, true);
//		MongoConfigUtil.setBatchSize(jobConf, 10000);
//		MongoConfigUtil.setLimit(jobConf, 5);
		
		FileSystem fs = FileSystem.get(jobConf);
		deleteExistedDir(fs, new Path("/home/webuser/dmp/alex/mongo"), true);
		Path out = new Path("/home/webuser/dmp/alex/mongo");
		
		final Job job = new Job(jobConf, "alex_mongo_db_log");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(MongoDbDriver.class);
		job.setMapperClass(MongoDbMapper.class);
		job.setReducerClass(MongoDbReducer.class);
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);
		
		
		job.setMapSpeculativeExecution(true);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
//		String alllog = analyzerPathAlllog;
//		Calendar calendar = Calendar.getInstance();
//		
//		JobConf jobConf = new JobConf();
//		jobConf.setNumMapTasks(8);
//		jobConf.set("mapred.max.split.size","200388608");
//		jobConf.set("mapred.min.split.size","200388608");
//		jobConf.set("mapred.child.java.opts", "-Xmx2g");
//		jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");
//		jobConf.set("mapred.compress.map.output", "true");
//		jobConf.set("spring.profiles.active", "stg");
//		MongoConfigUtil.setInputURI(jobConf,"mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.user_detail"); 
////		MongoConfigUtil.setOutpu (jobConf,"mongodb://192.168.1.37:27017/microblog_part.hadoop_out");
//		
//		// hdfs
//		Configuration conf = new Configuration();
//		conf.set("hadoop.job.ugi", jobUgi);
//		conf.set("fs.defaultFS", hdfsPath);
//		conf.set("mapreduce.jobtracker.address", tracker);
//		conf.set("mapreduce.map.output.compress.codec", codec);
//		conf.set("mapreduce.map.speculative", mapredExecution);
//		conf.set("mapreduce.reduce.speculative", mapredReduceExecution);
//		conf.set("mapreduce.task.timeout", mapredTimeout);
//		conf.set("mapred.child.java.opts", "-Xmx4072m");
//		conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4072m");
//		conf.set("mapred.max.split.size","128388608");
//		conf.set("mapred.min.split.size","128388608");
//		conf.set("mapreduce.min.split.size","128388608");
//		conf.set("mapreduce.max.split.size","128388608");
//		conf.set("dfs.namenode.fs-limits.min-block-size","1048576");
//		conf.set("dfs.namenode.fs-limits.max-blocks-per-file","1048576");
//		
//		
//		
//		if(calendar.get(Calendar.HOUR_OF_DAY) == 0){
//			calendar.add(Calendar.DAY_OF_MONTH, -1);
//			conf.set("job.date",sdf1.format(calendar.getTime()));
//			jobConf.set("job.date",sdf1.format(calendar.getTime()));
//		}else{
//			conf.set("job.date",sdf1.format(calendar.getTime()));
//			jobConf.set("job.date",sdf1.format(calendar.getTime()));
//		}
//		Date date = new Date();
//		
//		// file system
//		conf.set("spring.profiles.active", "stg");
//		FileSystem fs = FileSystem.get(conf);
//
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		// job
//		log.info("----job start----");
//
//		Job job = new Job(jobConf, "mongoDB_record_log " + sdf.format(date));
//		job.setJarByClass(MongoDbDriver.class);
//		job.setMapperClass(MongoDbMapper.class);
//		job.setReducerClass(MongoDbReducer.class);
//		job.setInputFormatClass(MongoInputFormat.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(1);
//		job.setMapSpeculativeExecution(false);
//		
//		
//		StringBuffer alllogOpRange = new StringBuffer();
//		alllogOpRange.append(analyzerPathAlllog);
//		alllogOpRange.append(sdf1.format(date));
////			/home/webuser/akb/storedata/alllog/2017-10-01/00
//		String timePath  = "";
//		Calendar calendar2 = Calendar.getInstance();
//		if(calendar2.get(calendar2.HOUR_OF_DAY) == 0){
//			calendar2.add(calendar2.DAY_OF_MONTH, -1); 
//			timePath = sdf1.format(calendar2.getTime())+"/23";
//		}else {
//			if(String.valueOf(calendar2.get(calendar2.HOUR_OF_DAY) - 1).length() < 2){
//				timePath = sdf1.format(calendar2.getTime()) +"/"+ "0"+(calendar.get(calendar2.HOUR_OF_DAY) - 1);
//			}else{
//				timePath = sdf1.format(calendar2.getTime()) +"/"+ (calendar.get(calendar2.HOUR_OF_DAY) - 1);
//			}
//		}
////			//輸入
////			String adLogClassPpath = "/home/webuser/analyzer/storedata/alllog/2018-03-20";
////			//輸出
////			String bessieTempPath = "/home/webuser/dmp/adLogClassStg/2018-03-20/day";
//			//輸入
//			String adLogClassPpath = "/home/webuser/akb/storedata/alllog/"+timePath;
//			//輸出
//			String bessieTempPath = "/home/webuser/dmp/adLogClassPrd/categorylog/"+timePath;
//			
//			//hdfs存在則刪除
//			deleteExistedDir(fs, new Path("/home/webuser/dmp/adLogClassPrd/categorylog/mongo_db_log"), true);
//			
//			log.info(">>>>>>INPUT PATH:"+adLogClassPpath);
//			log.info(">>>>>>OUTPUT PATH:"+bessieTempPath);
//			
//			FileOutputFormat.setOutputPath(job, new Path("/home/webuser/dmp/adLogClassPrd/categorylog/mongo_db_log"));
////			FileInputFormat.addInputPaths(job, adLogClassPpath);
//
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
				"/home/webuser/dmp/webapps/analyzer/lib/mongo-hadoop-core-2.0.2.jar",

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
		log.info("====driver start====");
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		MongoDbDriver mongoDbDriver = (MongoDbDriver) ctx.getBean(MongoDbDriver.class);
		mongoDbDriver.drive();
		log.info("====driver end====");
	}

}
