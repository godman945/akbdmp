package com.pchome.dmp.mapreduce.crawlbreadcrumb;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class CrawlBreadCrumbDriver {
	
	Log log = LogFactory.getLog(CrawlBreadCrumbDriver.class);
	
	@Value("${hpd11.fs.default.name}")
	private String hdfsPath;
	
	@Value("${crawlBreadCrumb.urls.path}")
	private String crawlBreadCrumbUrlPath;
	
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
	


	public void drive(String dateStr) throws Exception {

		if(StringUtils.isBlank(crawlBreadCrumbUrlPath)){
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        log.info("----job start----");

        Job job = new Job(conf, "crawlBreadCrumb " + sdf.format(new Date()));
        job.setJarByClass(CrawlBreadCrumbDriver.class);
        job.setMapperClass(CrawlBreadCrumbMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CrawlBreadCrumbReducerTest.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
    }


	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		CrawlBreadCrumbDriver crawlBreadCrumbDriver = (CrawlBreadCrumbDriver) ctx.getBean(CrawlBreadCrumbDriver.class);
		
		String date = "";
        if(args.length ==1 && args[0].matches("\\d{4}-\\d{2}-\\d{2}") ) {
        	date = args[0];
        	crawlBreadCrumbDriver.drive(date);
        } else {
            return;
        }
        

	}

}
