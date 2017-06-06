package com.pchome.dmp.mapreduce.category;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.hadoop.mapreduce.LzoTextInputFormat;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

import net.minidev.json.JSONObject;

@Component
public class CategoryDriver {

//	private static String log4jPath = "/home/webuser/dmp/webapps/analyzer/src/config/log4j/Log4j_Category.xml";

	private static Log log = LogFactory.getLog(CategoryDriver.class);
	
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
	
	@Value("${kafka.metadata.broker.list}")
	private String kafkaMetadataBrokerList;
	
	@Value("${kafka.acks}")
	private String kafkaAcks;
	
	@Value("${kafka.retries}")
	private String kafkaRetries;
	
	@Value("${kafka.batch.size}")
	private String kafkaBatchSize;
	
	@Value("${kafka.linger.ms}")
	private String kafkaLingerMs;
	
	@Value("${kafka.buffer.memory}")
	private String kafkaBufferMemory;
	
	@Value("${kafka.serializer.class}")
	private String kafkaSerializerClass;
	
	@Value("${kafka.key.serializer}")
	private String kafkaKeySerializer;
	
	@Value("${kafka.value.serializer}")
	private String kafkaValueSerializer;
	
	List<JSONObject> kafkaList = new ArrayList<>();
	
	Producer<String, String> producer = null;

	public void drive(String dateStr) throws Exception {

		String alllog = analyzerPathAlllog;
//		log.info("alllog " + alllog);
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

		conf.set("job.date", (dateStr.matches("\\d{4}-\\d{2}-\\d{2}") || dateStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}"))?dateStr.substring(0, 10):"");
		System.out.println( "job.date: " + dateStr.substring(0, 10) );

		// file system
		FileSystem fs = FileSystem.get(conf);

		// date format
		//SimpleDateFormat hdf = new SimpleDateFormat("yyyy-MM-dd" + Path.SEPARATOR + "HH");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String alllogStr = null;
		Path alllogDir = null;
		Path alllogPath = null;

		// path
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
				
				//add kafka jar 
                "/home/webuser/dmp/alex/lib/kafka-clients-0.9.0.0.jar",
                "/home/webuser/dmp/alex/lib/kafka_2.11-0.9.0.0.jar",
                "/home/webuser/dmp/alex/lib/slf4j-api-1.7.19.jar",
                "/home/webuser/dmp/alex/lib/slf4j-log4j12-1.7.6.jar",
                "/home/webuser/dmp/alex/lib/json-smart-2.3.jar",
                "/home/webuser/dmp/alex/lib/asm-1.0.2.jar"  
		};		//@@ add
		
		// job
		log.info("----job start----");

		Job job = new Job(conf, "dmp_Category_TEST " + sdf.format(new Date()));
		job.setJarByClass(CategoryDriver.class);
		job.setMapperClass(CategoryMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CategoryReducer.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		//job.setOutputFormatClass(NullOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/home/webuser/dmp/crawlUrls_TEST"));//home/webuser/dmp/crawlUrls		bessie
													   

		if ( dateStr.matches("\\d{4}-\\d{2}-\\d{2}") ) {
			//opRange.add(Calendar.DAY_OF_YEAR, -1);
			StringBuffer alllogOpRange = new StringBuffer();

			boolean testFlag = Boolean.parseBoolean( inputPathTestingFlag );
			log.info("testFlag=" + testFlag);
			if( testFlag ) {
				// testData:
				alllogOpRange.append( inputPathTestingPath );
			}
			else {
				// formal Data: /home/webuser/analyzer/storedata/alllog/
				alllogOpRange.append(analyzerPathAlllog );
				alllogOpRange.append( dateStr );

			}

			FileInputFormat.addInputPaths(job, alllogOpRange.toString());
			log.info("alllogOpRange:" + alllogOpRange);
		} else if( dateStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}") ) {
			alllogStr = alllog + dateStr.replaceAll(" ", "/");
			alllogDir = new Path(alllogStr);
			if (fs.exists(alllogDir)) {
				alllogPath = alllogDir;
				FileInputFormat.addInputPath(job, alllogPath);
				log.info(alllogDir.toString() + " is exists");
			} else {
				log.info(alllogDir.toString() + " is not exists");
				return;
			}

		} else {
			log.info("date = null");
			return;
		}

		log.info("alllogPath=" + alllogPath);

		for (String jarPath: jarPaths) {
			DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
		}


		//delete old doc for specific date
		deleteMongoOldDoc(dateStr.substring(0, 10));
		

		if (job.waitForCompletion(true)) {
			log.info("Job is OK");
		} else {
			log.info("Job is Failed");
		}

	}

	private void deleteMongoOldDoc(String dateStr) throws Exception {
//		IClassCountDAO dao = ClassCountDAO.getInstance();
//		log.info("delete old doc for specific date: " + dao.deleteByDate(dateStr));
		
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaMetadataBrokerList);
		props.put("acks", kafkaAcks);
		props.put("retries", kafkaRetries);
		props.put("batch.size", kafkaBatchSize);
		props.put("linger.ms",kafkaLingerMs );
		props.put("buffer.memory", kafkaBufferMemory);
		props.put("serializer.class", kafkaSerializerClass);
		props.put("key.serializer", kafkaKeySerializer);
		props.put("value.serializer", kafkaValueSerializer);
		producer = new KafkaProducer<String, String>(props);
		
		
		JSONObject json = new JSONObject();
		json.put("type", "mongo");
		json.put("action", "delete");
		json.put("db", "dmp");
		json.put("collection", "class_count");
		json.put("column", "record_date");
		json.put("condition", dateStr);
		kafkaList.add(json);
		
		Future<RecordMetadata> deleteMongoOldDoc  = producer.send(new ProducerRecord<String, String>("TEST", "", kafkaList.toString()));
		while (!deleteMongoOldDoc.isDone()) {
		}
		
		log.info("CategoryDriver deleteMongoOldDoc send kafka: " + deleteMongoOldDoc);
	}

	public static void printUsage() {
		System.out.println("Usage(hour): [DATE] [HOUR]");
		System.out.println("Usage(day): [DATE]");
		System.out.println();
		System.out.println("[DATE] format: yyyy-MM-dd");
	}

	public static void main(String[] args) throws Exception {
//		DOMConfigurator.configure(log4jPath);
		
		log.info("====driver start====");

		String date = "";
		if (args.length ==1 ) {
			date = args[0];
		}
		else if (args.length == 2) {
			date = args[0] + " " + args[1];
		}
		else if (args.length != 0) {
			printUsage();
			return;
		}
		
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		CategoryDriver CategoryDriver = (CategoryDriver) ctx.getBean(CategoryDriver.class);

		CategoryDriver.drive(date);
		
		
//		
		/*
		CategoryDriver driver = new CategoryDriver();
		driver.drive(date);
		 */
		
		log.info("====driver end====");
	}

	
}
