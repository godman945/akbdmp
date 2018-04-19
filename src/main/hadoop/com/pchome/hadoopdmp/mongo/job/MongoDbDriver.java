package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.io.BSONWritable;
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
		JobConf jobConf = new JobConf();
		jobConf.set("spring.profiles.active", "stg");
		
		
		
//		MongoConfigUtil.setInputURI(jobConf,"mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.user_detail");
		MongoConfigUtil.setInputURI(jobConf,"mongodb://webuser:MonG0Dmp@mongodb.mypchome.com.tw/dmp.user_detail");
		
		
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lt", "2017-04-19")));
		obj.add(new BasicDBObject("user_info.type", "uuid"));
		andQuery.put("$and", obj);
		
		
		log.info(">>>>>>mongo:"+MongoConfigUtil.getInputURI(jobConf).getURI());
		log.info(">>>>>>mongo Query:"+andQuery.toString());
		
		MongoConfigUtil.setQuery(jobConf,andQuery);
		MongoConfigUtil.setLimit(jobConf,1000000);
		
		
		MongoConfigUtil.setInputFormat(jobConf, MongoInputFormat.class);
		MongoConfigUtil.setCreateInputSplits(jobConf, false);
		MongoConfigUtil.setMapper(jobConf, MongoDbMapper.class);
		
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
			String[] jarPaths = {
				"/home/webuser/dmp/webapps/analyzer/lib/commons-lang-2.6.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/commons-logging-1.1.1.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/log4j-1.2.15.jar",
				"/home/webuser/dmp/webapps/analyzer/lib/mongo-java-driver-2.11.3.jar",
//				"/home/webuser/dmp/webapps/analyzer/lib/mongo-java-driver-3.6.3.jar",
				
				
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
//		Mongo m = new Mongo("mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.user_detail");  
//		DB db = m.getDB( "dmp" );  
//		DBCollection dBCollection = db.getCollection("user_detail");
////		System.out.println(dBCollection.count());
////		
////		
//////		Mongo m = new Mongo("mongodb.mypchome.com.tw");  
//////		DB db = m.getDB("dmp");
//////		db.authenticate("webuser", "MonG0Dmp".toCharArray());  
//////		DBCollection dBCollection = db.getCollection("user_detail");
////		
////		
//		
//		BasicDBObject andQuery = new BasicDBObject();
//		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
//		obj.add(new BasicDBObject("update_date", new BasicDBObject("$gte", "2017-04-19")));
//		obj.add(new BasicDBObject("user_info.type", "uuid"));
//		andQuery.put("$and", obj);
		
		
//		DBCursor cursor = dBCollection.find(andQuery).limit(1);
//		while(cursor.hasNext()) {
//			DBObject a = cursor.next();
//			System.out.println(a.get("_id"));
////			dBCollection.remove(a);
////			break;
//		}
		
		
		
		
//		BasicDBObject andQuery = new BasicDBObject();
//		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
//		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lte", "2017-04-18")));
////		obj.add(new BasicDBObject("user_info.type", "uuid"));
//		andQuery.put("$and", obj);
////		andQuery.put("update_date", new BasicDBObject("$lte", "2017-04-18"));
//		System.out.println(andQuery.toString());
//		DBCursor cursor = dBCollection.find(andQuery).limit(5);
//		System.out.println(cursor.count());
//		while(cursor.hasNext()) {
//			DBObject a = cursor.next();
//			System.out.println(a.get("_id"));
////			dBCollection.remove(a);
////			break;
//		}
		
		
		log.info("====driver start====");
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		MongoDbDriver mongoDbDriver = (MongoDbDriver) ctx.getBean(MongoDbDriver.class);
		mongoDbDriver.drive();
		log.info("====driver end====");
	}

}
