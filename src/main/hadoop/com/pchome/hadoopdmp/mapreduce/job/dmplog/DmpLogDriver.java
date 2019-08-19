package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
@Component
public class DmpLogDriver {

	private static Log log = LogFactory.getLog("DmpLogDriver");

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
			
			Configuration conf = new Configuration();
			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.mapreduce.LzoTextInputFormat");
			conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
			conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec");
			conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
			conf.set("mapred.compress.map.output", "true");
			conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
	        conf.set("spring.profiles.active", env);
	        conf.set("job.date",dmpDate);
	        conf.set("job.hour",dmpHour);
			
			
			
	        //輸入檔案
	        List<Path> listPath = new ArrayList<Path>();  
	        FileSystem fs = FileSystem.get(conf);
	        String hour = "";
	        if(dmpHour.equals("day")) {//計算整天
	  			for (int i = 0; i < 24; i++) {
	  				hour = "";
	  				if(i == 0) {
	  					hour = "00";
	  				}else if(String.valueOf(i).length() == 1) {
	  					hour = "0"+i;
	  				}else if(String.valueOf(i).length() == 2) {
	  					hour = String.valueOf(i);
	  				}
	  				//載入bu log file
	  				Path buPath = new Path("/druid_source/bu_log/"+dmpDate+"/"+hour);
	  				FileStatus[] buStatus = fs.listStatus(buPath); 
	  				for (FileStatus fileStatus : buStatus) {
	  					String pathStr = fileStatus.getPath().toString();
	  					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
	  					if(extensionName.equals("LZO")) {
	  						listPath.add(new Path(fileStatus.getPath().toString()));
	  					}
	  				}
	  				//載入kdcl log file
	  				Path kdclPath = new Path("/druid_source/kdcl_log/"+dmpDate);
			        FileStatus[] kdclStatus = fs.listStatus(kdclPath); 
					for (FileStatus fileStatus : kdclStatus) {
						String pathStr = fileStatus.getPath().toString();
						String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
						if(extensionName.equals("LZO")) {
							listPath.add(new Path(fileStatus.getPath().toString()));
						}
					}
					//載入pacl log file
					Path paclPath = new Path("/druid_source/pacl_log/"+dmpDate+"/"+hour);
			        FileStatus[] paclStatus = fs.listStatus(paclPath); 
					for (FileStatus fileStatus : paclStatus) {
						String pathStr = fileStatus.getPath().toString();
						String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
						if(extensionName.equals("LZO")) {
							listPath.add(new Path(fileStatus.getPath().toString()));
						}
					}
	  			}
	        }else {//計算小時
//	        	//載入bu log file
//		        Path buPath = new Path("/druid_source/bu_log/"+dmpDate+"/"+dmpHour);
//		        FileStatus[] buStatus = fs.listStatus(buPath); 
//				for (FileStatus fileStatus : buStatus) {
//					String pathStr = fileStatus.getPath().toString();
//					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
//					if(extensionName.equals("LZO")) {
//						listPath.add(new Path(fileStatus.getPath().toString()));
//					}
//				}
				//載入kdcl log file
		        Path kdclPath = new Path("/druid_source/kdcl_log/"+dmpDate+"/"+dmpHour);
		        FileStatus[] kdclStatus = fs.listStatus(kdclPath); 
				for (FileStatus fileStatus : kdclStatus) {
					String pathStr = fileStatus.getPath().toString();
					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
					if(extensionName.equals("LZO")) {
						listPath.add(new Path(fileStatus.getPath().toString()));
					}
				}
//				//載入pacl log file
//				Path paclPath = new Path("/druid_source/pacl_log/"+dmpDate+"/"+dmpHour);
//		        FileStatus[] paclStatus = fs.listStatus(paclPath); 
//				for (FileStatus fileStatus : paclStatus) {
//					String pathStr = fileStatus.getPath().toString();
//					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
//					if(extensionName.equals("LZO")) {
//						listPath.add(new Path(fileStatus.getPath().toString()));
//					}
//				}
	        }
			Path[] paths = new Path[listPath.size()];  
			listPath.toArray(paths);
			for (Path path : paths) {
				log.info(">>>>>>>>>>JOB INPUT PATH:"+path.toString());
			}
			
			
			
			Job job = new Job(conf, "dmp_log_"+ env + "_druid_test");
			job.setJarByClass(DmpLogDriver.class);
			job.setMapperClass(DmpLogMapper.class);
			job.setReducerClass(DmpLogReducer.class);
			job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.getConfiguration().set("mapreduce.output.basename", "druid_"+dmpDate+"_"+dmpHour);
			job.setNumReduceTasks(1); 
			
			
			if(env.equals("prd")) {
				deleteExistedDir(fs, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour), true);
				FileOutputFormat.setOutputPath(job, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour));
			}else {
				deleteExistedDir(fs, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour), true);
				FileOutputFormat.setOutputPath(job, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour));
			}
			log.info("JOB OUTPUT PATH:"+"/druid_source/dmp_output/"+dmpDate+"/"+dmpHour);
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setCompressOutput(job, true);  //job使用压缩  
	        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);  
			
	        
	    	String[] jarPaths = {
//					"/hadoop_jar/lib/commons-lang-2.6.jar",
//					"/hadoop_jar/lib/commons-logging-1.1.1.jar",
//					"/hadoop_jar/lib/log4j-1.2.15.jar",
//					"/hadoop_jar/lib/mongo-java-driver-2.11.3.jar",
//					"/hadoop_jar/lib/softdepot-1.0.9.jar",
//					"/hadoop_jar/lib/solr-solrj-4.5.0.jar",
//					"/hadoop_jar/lib/noggit-0.5.jar",
//					"/hadoop_jar/lib/httpcore-4.2.2.jar",
//					"/hadoop_jar/lib/httpclient-4.2.3.jar",
//					"/hadoop_jar/lib/httpmime-4.2.3.jar",
//					"/hadoop_jar/lib/mysql-connector-java-5.1.12-bin.jar",
//					"/hadoop_jar/lib/hadoop-lzo-0.4.20.jar",
//					
//					// add kafka jar
//					"/hadoop_jar/lib/kafka-clients-0.9.0.0.jar",
//					"/hadoop_jar/lib/kafka_2.11-0.9.0.0.jar",
//					"/hadoop_jar/lib/slf4j-api-1.7.19.jar",
//					"/hadoop_jar/lib/slf4j-log4j12-1.7.6.jar",
					"/hadoop_jar/lib/json-smart-2.3.jar",
//					"/hadoop_jar/lib/asm-1.0.2.jar" 
			}; 
			for (String jarPath : jarPaths) {
				DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
			}
	        
			
			String[] filePaths = {
				"/hadoop_file/pfp_ad_category_new.csv",
				"/hadoop_file/ClsfyGndAgeCrspTable.txt",
				"/hadoop_file/log4j.xml",
				"/hadoop_file/DMP_24h_category.csv",
				"/hadoop_file/DMP_Ruten_category.csv",
				"/hadoop_file/GeoLite2-City.mmdb",
				"/hadoop_file/ThirdAdClassTable.txt"
			};
			for (String filePath : filePaths) {
				DistributedCache.addCacheFile(new URI(filePath), job.getConfiguration());
			}
			
			
			if (job.waitForCompletion(true)) {
				log.info("Job1 is OK");
			} else {
				log.info("Job1 is Failed");
			}
			System.exit(job.waitForCompletion(true) ? 0 : 1);
				
			
//			JobConf jobConf = new JobConf();
//			jobConf.setNumMapTasks(10);
//			jobConf.set("mapred.max.split.size","9045728000"); //3045728 49 //3045728000 7
//			jobConf.set("mapred.min.split.size","3045728000"); //1015544 49 //1015544000 7
//			//ask推测执行
//			jobConf.set("mapred.map.tasks.speculative.execution","true");
//			jobConf.set("mapred.reduce.tasks.speculative.execution","true");
//			//JVM
//			jobConf.set("mapred.child.java.opts", "-Xmx4096M");
//			jobConf.set("mapreduce.map.memory.mb", "4096");
//			jobConf.set("mapreduce.reduce.memory.mb", "4096");
//			jobConf.set("mapreduce.job.running.map.limit", "100");
//			jobConf.set("spring.profiles.active", env);
//			jobConf.set("job.date",dmpDate);
//			jobConf.set("job.hour",dmpHour);
//			
//			// hdfs
//			Configuration conf = new Configuration();
//			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.mapreduce.LzoTextInputFormat");
//			conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
//			conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec");
//			conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
//			conf.set("mapred.compress.map.output", "true");
//			conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
//			conf.set("mapreduce.map.speculative", "false");
//			conf.set("mapreduce.reduce.speculative", "false");
//			conf.set("mapreduce.task.timeout", "0");
//			conf.set("mapred.map.tasks.speculative.execution","true");
//			conf.set("mapred.reduce.tasks.speculative.execution","true");
//			conf.set("mapred.child.java.opts", "-Xmx4096M");
//			conf.set("mapreduce.map.memory.mb", "4096");
//	        conf.set("mapreduce.map.java.opts", "-Xmx4096m");
//	        conf.set("mapreduce.reduce.memory.mb", "4096");
//	        conf.set("mapreduce.reduce.java.opts", "-Xmx4096m");

//			Configuration conf = new Configuration();
//			conf.set("mapreduce.map.output.compress.codec", "com.hadoop.mapreduce.LzoTextInputFormat");
//			conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
//			conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec");
//			conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
//			conf.set("mapred.compress.map.output", "true");
//			conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
//	        conf.set("spring.profiles.active", env);
//	        conf.set("job.date",dmpDate);
//	        conf.set("job.hour",dmpHour);
//			
//	        //輸入檔案
//	        List<Path> listPath = new ArrayList<Path>();  
//	        FileSystem fs = FileSystem.get(conf);
//	        String hour = "";
//	        if(dmpHour.equals("day")) {//計算整天
//	  			for (int i = 0; i < 24; i++) {
//	  				hour = "";
//	  				if(i == 0) {
//	  					hour = "00";
//	  				}else if(String.valueOf(i).length() == 1) {
//	  					hour = "0"+i;
//	  				}else if(String.valueOf(i).length() == 2) {
//	  					hour = String.valueOf(i);
//	  				}
//	  				//載入bu log file
//	  				Path buPath = new Path("/druid_source/bu_log/"+dmpDate+"/"+hour);
//	  				FileStatus[] buStatus = fs.listStatus(buPath); 
//	  				for (FileStatus fileStatus : buStatus) {
//	  					String pathStr = fileStatus.getPath().toString();
//	  					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
//	  					
//	  					if(extensionName.equals("LZO")) {
//	  						listPath.add(new Path(fileStatus.getPath().toString()));
//	  					}
//	  				}
//	  				//載入kdcl log file
//	  				Path kdclPath = new Path("/druid_source/kdcl_log/"+dmpDate);
//			        FileStatus[] kdclStatus = fs.listStatus(kdclPath); 
//					for (FileStatus fileStatus : kdclStatus) {
//						String pathStr = fileStatus.getPath().toString();
//						String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
//						if(extensionName.equals("LZO")) {
//							listPath.add(new Path(fileStatus.getPath().toString()));
//						}
//					}
//					//載入pacl log file
//					Path paclPath = new Path("/druid_source/pacl_log/"+dmpDate+"/"+hour);
//			        FileStatus[] paclStatus = fs.listStatus(paclPath); 
//					for (FileStatus fileStatus : paclStatus) {
//						String pathStr = fileStatus.getPath().toString();
//						String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
//						if(extensionName.equals("LZO")) {
//							listPath.add(new Path(fileStatus.getPath().toString()));
//						}
//					}
//	  			}
//	        }else {//計算小時
////	        	//載入bu log file
////		        Path buPath = new Path("/druid_source/bu_log/"+dmpDate+"/"+dmpHour);
////		        FileStatus[] buStatus = fs.listStatus(buPath); 
////				for (FileStatus fileStatus : buStatus) {
////					String pathStr = fileStatus.getPath().toString();
////					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
////					if(extensionName.equals("LZO")) {
////						listPath.add(new Path(fileStatus.getPath().toString()));
////					}
////				}
//				//載入kdcl log file
//		        Path kdclPath = new Path("/druid_source/kdcl_log/"+dmpDate+"/"+dmpHour);
//		        FileStatus[] kdclStatus = fs.listStatus(kdclPath); 
//				for (FileStatus fileStatus : kdclStatus) {
//					String pathStr = fileStatus.getPath().toString();
//					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
//					if(extensionName.equals("LZO")) {
//						listPath.add(new Path(fileStatus.getPath().toString()));
//					}
//				}
////				//載入pacl log file
////				Path paclPath = new Path("/druid_source/pacl_log/"+dmpDate+"/"+dmpHour);
////		        FileStatus[] paclStatus = fs.listStatus(paclPath); 
////				for (FileStatus fileStatus : paclStatus) {
////					String pathStr = fileStatus.getPath().toString();
////					String extensionName = pathStr.substring(pathStr.length()-3,pathStr.length()).toUpperCase();
////					if(extensionName.equals("LZO")) {
////						listPath.add(new Path(fileStatus.getPath().toString()));
////					}
////				}
//	        }
//			Path[] paths = new Path[listPath.size()];  
//			listPath.toArray(paths);
//			for (Path path : paths) {
//				log.info(">>>>>>>>>>JOB INPUT PATH:"+path.toString());
//			}
//			
//			Job job = new Job(conf, "dmp_log_"+ env + "_druid_test");
//			job.setJarByClass(DmpLogDriver.class);
//			job.setMapperClass(DmpLogMapper.class);
//			job.setReducerClass(DmpLogReducer.class);
//			job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(Text.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);
//			job.getConfiguration().set("mapreduce.output.basename", "druid_"+dmpDate+"_"+dmpHour);
//			job.setNumReduceTasks(1); 
//			
////			Job job = new Job(conf, "dmp_log_"+ env + "_druid_test");
////			job.setJarByClass(DmpLogDriver.class);
////			job.setMapperClass(DmpLogMapper.class);
////			job.setMapOutputKeyClass(Text.class);
////			job.setMapOutputValueClass(Text.class);
////			job.setReducerClass(DmpLogReducer.class);
////			job.setInputFormatClass(com.hadoop.mapreduce.LzoTextInputFormat.class);
////			job.setOutputKeyClass(Text.class);
////			job.setOutputValueClass(Text.class);
////			job.getConfiguration().set("mapreduce.output.basename", "druid_"+dmpDate+"_"+dmpHour);
////			job.setNumReduceTasks(1); 
////			job.setMapSpeculativeExecution(false);
//			
//			
//			if(env.equals("prd")) {
//				deleteExistedDir(fs, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour), true);
//				FileOutputFormat.setOutputPath(job, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour));
//			}else {
//				deleteExistedDir(fs, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour), true);
//				FileOutputFormat.setOutputPath(job, new Path("/druid_source/dmp_output/"+dmpDate+"/"+dmpHour));
//			}
//			log.info("JOB OUTPUT PATH:"+"/druid_source/dmp_output/"+dmpDate+"/"+dmpHour);
//			FileInputFormat.setInputPaths(job, paths);
//			FileOutputFormat.setCompressOutput(job, true);  //job使用压缩  
//	        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);  
//		
//	      //load jar path
//			String[] jarPaths = {
////					"/hadoop_jar/lib/commons-lang-2.6.jar",
////					"/hadoop_jar/lib/commons-logging-1.1.1.jar",
////					"/hadoop_jar/lib/log4j-1.2.15.jar",
////					"/hadoop_jar/lib/mongo-java-driver-2.11.3.jar",
////					"/hadoop_jar/lib/softdepot-1.0.9.jar",
////					"/hadoop_jar/lib/solr-solrj-4.5.0.jar",
////					"/hadoop_jar/lib/noggit-0.5.jar",
////					"/hadoop_jar/lib/httpcore-4.2.2.jar",
////					"/hadoop_jar/lib/httpclient-4.2.3.jar",
////					"/hadoop_jar/lib/httpmime-4.2.3.jar",
////					"/hadoop_jar/lib/mysql-connector-java-5.1.12-bin.jar",
////					"/hadoop_jar/lib/hadoop-lzo-0.4.20.jar",
////					
////					// add kafka jar
////					"/hadoop_jar/lib/kafka-clients-0.9.0.0.jar",
////					"/hadoop_jar/lib/kafka_2.11-0.9.0.0.jar",
////					"/hadoop_jar/lib/slf4j-api-1.7.19.jar",
////					"/hadoop_jar/lib/slf4j-log4j12-1.7.6.jar",
////					"/hadoop_jar/lib/json-smart-2.3.jar",
////					"/hadoop_jar/lib/asm-1.0.2.jar" 
//			}; 
//			for (String jarPath : jarPaths) {
//				DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
//			}
//	
//			String[] filePaths = {
//					"/hadoop_file/pfp_ad_category_new.csv",
//					"/hadoop_file/ClsfyGndAgeCrspTable.txt",
//					"/hadoop_file/log4j.xml",
//					"/hadoop_file/DMP_24h_category.csv",
//					"/hadoop_file/DMP_Ruten_category.csv",
//					"/hadoop_file/GeoLite2-City.mmdb",
//					"/hadoop_file/ThirdAdClassTable.txt"
//			};
//			for (String filePath : filePaths) {
//				DistributedCache.addCacheFile(new URI(filePath), job.getConfiguration());
//			}
//	
//			if (job.waitForCompletion(true)) {
//				log.info("Job1 is OK");
//			} else {
//				log.info("Job1 is Failed");
//			}
//			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
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
			
//			JSONObject layerJson = new JSONObject();
//			layerJson.put("A", "1");
//			layerJson.put("B", "2");
//			layerJson.put("C", "3");
//			System.out.println(layerJson);
//			
//			while(keys.hasNext()) {
//			    String key = keys.next();
//			    String value = layerJson.getAsString(key);
//			    
//			    System.out.println(key);
//			    System.out.println(value);
//			    
//			}
			
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
