package com.pchome.hadoopdmp.mapreduce.crawlbreadcrumb;

import java.net.URI;
import java.text.SimpleDateFormat;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class CrawlBreadCrumbDriver {
	
	Log log = LogFactory.getLog(CrawlBreadCrumbDriver.class);
	
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


	public void drive(String dateStr) throws Exception {
		System.out.println("---NEW---Kafka-----reducer-------Kafka-----reducer--------Kafka-------reducer---------Kafka------reducer------Kafka-----reducer--------Kafka------reducer----------Kafka----reducer--------Kafka-------reducer------Kafka------reducer------- ");
		System.out.println("------------------2016-12-14-------------------------------crawlBreadCrumbUrlsPath-----------------------: "+crawlBreadCrumbUrlsPath);
		System.out.println("------------------2016-12-14------------------------------hdfsPath-----------------------: "+hdfsPath);

    	StringBuffer urlToCrawlPath = new StringBuffer()
//    			.append("/home/webuser/dmp/crawlBreadCrumb/urls/2015_testlog.txt")//bessie delete
    			.append(crawlBreadCrumbUrlsPath)
    			.append("/")
    			.append(dateStr);
    	log.info("urlToCrawlPath: " + urlToCrawlPath);
    	//urlToCrawlPath = new StringBuffer().append( PropertyUtil.getProperty("crawlBreadCrumb.urls.path") ).append("/").append("test");		//test
    	if (StringUtils.isBlank( urlToCrawlPath.toString() )) {
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

        // file system
        FileSystem fs = FileSystem.get(conf);
//
//        // date format
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//        SimpleDateFormat hdf = new SimpleDateFormat("yyyy-MM-dd" + Path.SEPARATOR + "HH");  //@@ 2015-10-19/15
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        //Calendar calendar = Calendar.getInstance();
//        Calendar before = Calendar.getInstance();
//
//        String alllogStr = null;
//        Path alllogDir = null;
//        Path alllogPath = null;
//
//        log.info("alllogPath=" + alllogPath);
//
//        String jobDate = df.format(before.getTime());
//        String jobHour = String.valueOf(before.get(Calendar.HOUR_OF_DAY));

        // conf param
        //conf.set("jdbcDriver", PropertyUtil.getProperty("jdbcDriver"));
        //conf.set("host", PropertyUtil.getProperty("host"));
        //conf.set("port", PropertyUtil.getProperty("port"));
        //conf.set("dbtype", PropertyUtil.getProperty("dbtype"));
        //conf.set("database", PropertyUtil.getProperty("database"));
        //conf.set("user", PropertyUtil.getProperty("user"));
        //conf.set("password", PropertyUtil.getProperty("password"));
//        conf.set("job.date", jobDate);
//        conf.set("job.hour", jobHour);

//        for (EnumSift enumSift: EnumSift.values()) {
//            conf.set(enumSift.getPathName(), PropertyUtil.getProperty(enumSift.getPathName()));
//        }

        // path
        String[] jarPaths = {
        		"/home/webuser/dmp/lib/commons-lang-2.6.jar",
                "/home/webuser/dmp/lib/commons-logging-1.1.1.jar",
                "/home/webuser/dmp/lib/log4j-1.2.15.jar",
                "/home/webuser/dmp/lib/mongo-java-driver-2.11.3.jar",
                "/home/webuser/dmp/lib/softdepot-1.0.9.jar",
                "/home/webuser/dmp/lib/solr-solrj-4.5.0.jar",
                "/home/webuser/dmp/lib/noggit-0.5.jar",
                //"/home/webuser/dmp/lib/httpcore-4.2.2.jar",
                //"/home/webuser/dmp/lib/httpclient-4.2.3.jar",
                //"/home/webuser/dmp/lib/httpmime-4.2.3.jar",
                //"/home/webuser/dmp/lib/httpcore-4.3.jar",
                //"/home/webuser/dmp/lib/httpclient-4.3.1.jar",
                "/home/webuser/dmp/lib/mysql-connector-java-5.1.12-bin.jar",
                "/home/webuser/dmp/lib/htmlunit-2.16-OSGi.jar",
                "/home/webuser/dmp/lib/java-json.jar",
                "/home/webuser/dmp/lib/jsoup-1.7.2.jar",
                
                //add kafka jar 
                "/home/webuser/dmp/alex/lib/kafka-clients-0.9.0.0.jar",
                "/home/webuser/dmp/alex/lib/kafka_2.11-0.9.0.0.jar",
                "/home/webuser/dmp/alex/lib/slf4j-api-1.7.19.jar",
                "/home/webuser/dmp/alex/lib/slf4j-log4j12-1.7.6.jar",
                "/home/webuser/dmp/alex/lib/json-smart-2.3.jar",
                "/home/webuser/dmp/alex/lib/asm-1.0.2.jar"  
            };		
        
        

        // job
        log.info("----job start----");

        Job job = new Job(conf, "crawlBreadCrumb " + sdf.format(new Date()));
        job.setJarByClass(CrawlBreadCrumbDriver.class);
        job.setMapperClass(CrawlBreadCrumbMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CrawlBreadCrumbReducer.class);
        //job.setInputFormatClass(LzoTextInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        job.setOutputFormatClass(NullOutputFormat.class);	//bf
        //FileOutputFormat.setOutputPath(job, new Path("/home/webuser/crawlBreadCrumb/output"));	//ok

        //job.setOutputFormat(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(OutputFormat.class);

        
        
        Path urlToCrawlHdfsPath = new Path(urlToCrawlPath.toString());
//        Path urlToCrawlHdfsPath = new Path("/home/webuser/dmp/crawlBreadCrumb/urls/2015-12-20/2015_testlog.txt");//bessie delete
        if( fs.exists(urlToCrawlHdfsPath) ) {
        	FileInputFormat.addInputPath(job, urlToCrawlHdfsPath);
        	log.info(urlToCrawlHdfsPath.toString() + " exists");
        } else {
        	log.info(urlToCrawlHdfsPath.toString() + " does not exist");
        	return;
        }


        for (String jarPath: jarPaths) {
            DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
        }

        DistributedCache.addCacheFile(new URI( hdfsPath +"/home/webuser/dmp/crawlBreadCrumb/data/pfp_ad_category_new.csv"), job.getConfiguration());

        //delete old doc for specific date
        //deleteMongoOldDoc(dateStr.substring(0, 10));

        if (job.waitForCompletion(true)) {
        	log.info("Job is OK");
        } else {
        	log.info("Job is Failed");
        }
    }

	/*mark by bessie
	private static void deleteMongoOldDoc(String dateStr) throws Exception {
    	IClassUrlDAO dao = ClassUrlDAO.getInstance();
    	log.info("delete old doc for specific date: " + dao.deleteByDate(dateStr));
    }
	 */
	
	public static void printUsage() {
        System.out.println("Usage(day): [DATE]");
        System.out.println();
        System.out.println("[DATE] format: yyyy-MM-dd");
    }
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		CrawlBreadCrumbDriver crawlBreadCrumbDriver = (CrawlBreadCrumbDriver) ctx.getBean(CrawlBreadCrumbDriver.class);
		
		/*mark by bessie
		DOMConfigurator.configure(log4jPath);
		org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF);
		log.info("====driver start====");
		 */
		
		String date = "";
        if(args.length ==1 && args[0].matches("\\d{4}-\\d{2}-\\d{2}") ) {
        	date = args[0];
        	crawlBreadCrumbDriver.drive(date);
        } else {
            printUsage();
            return;
        }
        
        /*mark by bessie
        log.info("====driver end====");
        */

	}

}
