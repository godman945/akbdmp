package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
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

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.soft.util.MysqlUtil;

import net.minidev.json.JSONObject;

@Component
public class PaclLogConverCountDriver {

	private static Log log = LogFactory.getLog("PaclLogConverCountDriver");

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

	@Value("${akb.pacl.all}")
	private String akbPacLoglAll;
	
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private final static int convertDay = 28;
	
	private String logInputPath;
	
	private String outPath;
	
	public static StringBuffer effectPaclPfpUser = new StringBuffer(); 
	
	public void drive(String env,String jobDate) throws Exception {
		try {
			JobConf jobConf = new JobConf();
			
			jobConf.setNumMapTasks(5);
			
			jobConf.set("mapred.max.split.size","100457280000"); //3045728 49 //3045728000 7
			jobConf.set("mapred.min.split.size","100457280000"); //1015544 49 //1015544000 7
			
			//ask推测执行
			jobConf.set("mapred.map.tasks.speculative.execution","true");
			jobConf.set("mapred.reduce.tasks.speculative.execution","true");
//			//JVM
			jobConf.set("mapred.child.java.opts", "-Xmx4048M");
			
			jobConf.set("mapreduce.map.memory.mb", "4096");
			jobConf.set("mapreduce.reduce.memory.mb", "8192");
			
//		    jobConf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");
			
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
			
			//JVM
			conf.set("mapred.child.java.opts", "-Xmx4048M");
//			conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2g");
			Calendar cal = Calendar.getInstance();  
			if(StringUtils.isNotBlank(jobDate)){
				cal.setTime(sdf.parse(jobDate));
				jobConf.set("job.date", jobDate);
			}else{
				cal.setTime(new Date());
				jobConf.set("job.date", sdf.format(new Date()));
			}
			
			FileSystem fs = FileSystem.get(conf);
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date date = new Date();
			// job
			log.info("----job1 start---- jobDate:"+jobDate);
	
			Job job = new Job(jobConf, "dmp_conv_count_"+ env + "_" + sdf2.format(date));
			job.setJarByClass(PaclLogConverCountDriver.class);
			job.setMapperClass(PaclLogConverCountMapper.class);
			job.setReducerClass(PaclLogConverCountReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1); 
			job.setMapSpeculativeExecution(false);
			job.setInputFormatClass(LzoTextInputFormat.class);
			Path inPath = new Path("/home/webuser/pa/storedata/alllog/"+sdf.format(cal.getTime()));
			FileStatus[] status = fs.listStatus(inPath);  
			List<Path> list = new ArrayList<Path>();  
			for (FileStatus fileStatus : status) {  
			    if (fs.getFileStatus(fileStatus.getPath()).isDir()) {  
			        list.add(fileStatus.getPath());
			        log.info("path:"+fileStatus.getPath());
			    }  
			}  
			Path[] paths = new Path[list.size()];  
			list.toArray(paths);  
			outPath = "/home/webuser/alex/pacl_output";
			//hdfs存在則刪除
			deleteExistedDir(fs, new Path(outPath), true);
				
			log.info(">>>>>>Job1 INPUT PATH:"+logInputPath);
			log.info(">>>>>>Job1 OUTPUT PATH:"+outPath);
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
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
					"/home/webuser/dmp/webapps/analyzer/lib/json-20160810.jar",
					
					// add kafka jar
					"/home/webuser/dmp/webapps/analyzer/lib/kafka-clients-0.9.0.0.jar",
					"/home/webuser/dmp/webapps/analyzer/lib/kafka_2.11-0.9.0.0.jar",
					"/home/webuser/dmp/webapps/analyzer/lib/slf4j-api-1.7.19.jar",
					"/home/webuser/dmp/webapps/analyzer/lib/slf4j-log4j12-1.7.6.jar",
					"/home/webuser/dmp/webapps/analyzer/lib/json-smart-2.3.jar",
					"/home/webuser/dmp/webapps/analyzer/lib/asm-1.0.2.jar",
					// add hbase jar
					"/home/webuser/dmp/webapps/analyzer/lib/hbase-client-1.4.5.jar"
			}; 
			for (String jarPath : jarPaths) {
				Path hadoopJarPath = new Path(jarPath);
				
				
				FileStatus[] files = fs.listStatus(hadoopJarPath);
				for (FileStatus fileStatus : files) {
					log.info("hadoopJarPath:"+fileStatus.getPath());
				}
				
			
				
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

//			log.info("----job2 start----");
//			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
//			String jdbcDriver = "com.mysql.jdbc.Driver";
//			String user = "keyword";
//			String password =  "K1y0nLine";
//			MysqlUtil mysqlUtil = MysqlUtil.getInstance();
//			mysqlUtil.setConnection(url, user, password);
//			Calendar effectCalendar = Calendar.getInstance();
//			effectCalendar.setTime(cal.getTime());
//			effectCalendar.add(Calendar.DATE, - convertDay);
//			String effectDate = sdf.format(effectCalendar.getTime());
//			StringBuffer sql = new StringBuffer();
//			sql.append(" SELECT c.pfp_customer_info_id  ");
//			sql.append(" FROM   (SELECT customer_info_id  ");
//			sql.append(" FROM   pfp_ad_action_report  ");
//			sql.append(" WHERE  1 = 1  ");
//			sql.append(" AND ad_pvclk_date >= '").append(effectDate).append("'");
//			sql.append(" GROUP  BY customer_info_id)a  ");
//			sql.append(" RIGHT JOIN pfp_code_convert c  ");
//			sql.append(" ON a.customer_info_id = c.pfp_customer_info_id  ");
//			sql.append(" AND c.convert_status = 1  ");
//			sql.append(" GROUP  BY pfp_customer_info_id  ");
//			ResultSet resultSet = mysqlUtil.query(sql.toString());
//			while(resultSet.next()){
//				String pfpCustomerInfoId = resultSet.getString("pfp_customer_info_id");
//				effectPaclPfpUser.append(pfpCustomerInfoId).append(",");
//			}
//			mysqlUtil.closeConnection();
//			jobConf.set("effectPaclPfpUser", effectPaclPfpUser.toString());
//			
//			Job job2 = new Job(jobConf, "dmp_conv2_"+ env + "_" + sdf2.format(date));
//			for (String jarPath : jarPaths) {
//				DistributedCache.addArchiveToClassPath(new Path(jarPath), job2.getConfiguration(), fs);
//			}
//			for (String filePath : filePaths) {
//				DistributedCache.addCacheFile(new URI(filePath), job2.getConfiguration());
//			}
//			
//			job2.setJarByClass(PaclLogConverCountDriver.class);
//			job2.setMapperClass(PaclLogConverCountMapper.class);
//			job2.setReducerClass(PaclLogConverCountReducer2.class);
////			job2.setCombinerClass(PaclLogConvertCombiner.class);
//			job2.setMapOutputKeyClass(Text.class);
//			job2.setMapOutputValueClass(Text.class);
//			job2.setInputFormatClass(LzoTextInputFormat.class);
//			job2.setOutputKeyClass(Text.class);
//			job2.setOutputValueClass(Text.class);
//			job2.setNumReduceTasks(1);//1個reduce 
//			job2.setMapSpeculativeExecution(false);
//			
//			
//			//STG
////			Path inPath = new Path("/home/webuser/akbstg/storedata/alllog/"+sdf.format(cal.getTime()));
//			list = new ArrayList<Path>();  
//			for (int j = 0; j < convertDay; j++) {
//				cal.add(Calendar.DATE, -1);  
//				if(j < convertDay){
//					inPath = new Path("/home/webuser/akbstg/storedata/alllog/"+sdf.format(cal.getTime()));
//					status = fs.listStatus(inPath);  
//					for (FileStatus fileStatus : status) {  
//					    if (fs.getFileStatus(fileStatus.getPath()).isDir()) {  
//					        list.add(fileStatus.getPath());
//					        log.info("path:"+fileStatus.getPath());
//					    }  
//					}  
//				}
//			}
//			Path paclPath = new Path("/home/webuser/alex/pacl_output/");  
//			list.add(paclPath);
//			log.info("path:/home/webuser/alex/pacl_output/");
//
//			paths = new Path[list.size()];  
//			list.toArray(paths);  
//			FileInputFormat.setInputPaths(job2, paths);
//			outPath = "/home/webuser/alex/pacl_output2";
//			//hdfs存在則刪除
//			deleteExistedDir(fs, new Path(outPath), true);
//			FileOutputFormat.setOutputPath(job2, new Path(outPath));
//			if (job2.waitForCompletion(true)) {
//				log.info("Job2 is OK");
//				
//			} else {
//				log.info("Job2 is Failed");
//			}
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
		log.info("====driver start====");
		if(args.length > 0 && (args[0].equals("prd") || args[0].equals("stg")) ){
			if(args[0].equals("prd")){
				System.setProperty("spring.profiles.active", "prd");
			}else{
				System.setProperty("spring.profiles.active", "stg");
			}
			
			String jobDate ="";
			if(args.length == 2){
				jobDate = args[1];
			}
			
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			PaclLogConverCountDriver paclLogConverCountDriver = (PaclLogConverCountDriver) ctx.getBean(PaclLogConverCountDriver.class);
			paclLogConverCountDriver.drive(args[0],jobDate);
			log.info("====driver end====");
		}else{
			log.info("==== args[0] must be 'prd' or 'stg' ====");
		}
	}
}
