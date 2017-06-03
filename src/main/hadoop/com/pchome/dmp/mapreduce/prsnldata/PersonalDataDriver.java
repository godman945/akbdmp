package com.pchome.dmp.mapreduce.prsnldata;


import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.xml.DOMConfigurator;
import org.springframework.beans.factory.annotation.Value;

import com.hadoop.mapreduce.LzoTextInputFormat;


public class PersonalDataDriver {
	private static Log log = LogFactory.getLog(PersonalDataDriver.class);

	private static String log4jPath = "/home/webuser/dmp/webapps/analyzer/src/config/log4j/log4j_PersonalData.xml";
	
//	private static String hdfsProperty = "/home/webuser/dmp/webapps/analyzer/src/config/prop/hdfs.properties";
//	private static String mongodbProperty = "/home/webuser/dmp/webapps/analyzer/src/config/prop/mongodb.properties";
//	private static String pathProperty = "/home/webuser/dmp/webapps/analyzer/src/config/prop/path.properties";

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
	
	@Value("${akb.path.alllog}")
	private String akbPathAlllog;
	
	@Value("${test.data.personalInfo.flag}")
	private String testDataPersonalInfoFlag;
	
	@Value("${test.data.personalInfo.path}")
	private String testDataPersonalInfoPath;
	
	@Value("${personalDataPhase1.output.path}")
	private String personalDataPhase1OutputPath;
	
	@Value("${personalDataPhase2.output.path}")
	private String personalDataPhase2OutputPath;
	

	public void drive(String specTime) throws Exception {

		String dataRootPath = akbPathAlllog;

		boolean testFlag = false;
		if( StringUtils.isNotBlank(testDataPersonalInfoFlag) ) {
			testFlag = Boolean.parseBoolean(testDataPersonalInfoFlag);
		}
		if( testFlag ) {
			dataRootPath = testDataPersonalInfoPath;
		}

		log.info("dataRootPath " + dataRootPath);
		if (StringUtils.isBlank(dataRootPath)) {
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
		conf.set("job.date", specTime.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}")?specTime.substring(0, 10):"");
		log.info( "job.date: " + specTime.substring(0, 10) );

		// file system
		FileSystem fs = FileSystem.get(conf);

		// delete already existed output dir from hdfs
		log.info("delete OutputPath1:" + deleteExistedDir(fs, new Path(personalDataPhase1OutputPath), true) );	//PropertyUtil.getProperty("personalDataPhase1.output.path")
		log.info("delete OutputPath2:" + deleteExistedDir(fs, new Path(personalDataPhase2OutputPath), true) );	//PropertyUtil.getProperty("personalDataPhase2.output.path")

		// date format
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
				"/home/webuser/dmp/webapps/analyzer/lib/mysql-connector-java-5.1.12-bin.jar"
		};

		// job
		log.info("----job start----");

//		Job job = new Job(conf, "dmp_PersonalData_Phase1 " + sdf.format(new Date()));
		Job job = new Job(conf, new StringBuffer().append("dmp_PersonalData_Phase1 ").append("rDate_").append(specTime.replaceAll(" ","_")).toString());
		job.setJarByClass(PersonalDataDriver.class);
		job.setMapperClass(PersonalDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PersonalDataReducer.class);
		job.setInputFormatClass(LzoTextInputFormat.class);
//		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(8);

		FileOutputFormat.setOutputPath(job, new Path(personalDataPhase1OutputPath));

		if( specTime.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}") ) {
			Path dataFullPath;
			if( testFlag ) {
				dataFullPath = new Path( dataRootPath );
			} else {
				dataFullPath = new Path( dataRootPath + specTime.replaceAll(" ", "/") );
			}

			if (fs.exists( dataFullPath )) {
				FileInputFormat.addInputPath(job, dataFullPath);
				log.info(dataFullPath.toString() + " is exists");
			} else {
				log.info(dataFullPath.toString() + " is not exists");
				return;
			}

		} else {
			log.info("specTime = null");
			return;
		}

		for (String jarPath: jarPaths) {
			DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
		}


		if (job.waitForCompletion(true)) {
			log.info("Phase1 Job is OK");
		} else {
			log.info("Phase1 Job is Failed");
			return;
		}

		// Phase2
		log.info("...... Phase2 start ......");

//		job = new Job(conf, "dmp_PersonalData_Phase2 " + sdf.format(new Date()));
		job = new Job(conf, new StringBuffer().append("dmp_PersonalData_Phase2 ").append("rDate_").append(specTime.replaceAll(" ","_")).toString());
		job.setJarByClass(PersonalDataDriver.class);
		job.setMapperClass(PersonalDataPhase2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(PersonalDataPhase2Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(NullOutputFormat.class);	// org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPaths(job, personalDataPhase1OutputPath);
		FileOutputFormat.setOutputPath(job, new Path(personalDataPhase2OutputPath));

		for (String jarPath: jarPaths) {
			DistributedCache.addArchiveToClassPath(new Path(jarPath), job.getConfiguration(), fs);
		}

		DistributedCache.addCacheFile(new URI(hdfsPath+"/home/webuser/dmp/readingdata/ClsfyGndAgeCrspTable.txt"), job.getConfiguration());

		if (job.waitForCompletion(true)) {
			log.info("Phase2 Job is OK");
		} else {
			log.info("Phase2 Job is Failed");
			return;
		}
	}


	public static void printUsage() {
		System.out.println("Usage: [DATE] [HOUR]");
		System.out.println("Ex: 2016-11-30 13");
	}

	public static void main(String[] args) throws Exception {
		DOMConfigurator.configure(log4jPath);

		log.info("====driver start====");

		String specTime = "";
		if(args.length ==2
				&& args[0].matches("\\d{4}-\\d{2}-\\d{2}")
				&& args[1].matches("\\d{2}") ) {
			specTime = new StringBuffer().append(args[0]).append(" ").append(args[1]).toString();
		} else {
			System.out.println("arg error");
			printUsage();
			return;
		}

		if(true) {
			System.out.println("specTime:" + specTime);

			// test
//			KdclStatisticsSourceDAO dao = new KdclStatisticsSourceDAO();
//			dao.dbInit();
//			dao.insert("memid", "member", "personal_info", "Y", AncestorJob.memCatedCnt, record_date);		//pcid -> memid		2 -> personal_info
//			System.out.println( dao.select("memid", "member", "personal_info", "Y", "2016-11-29") );
//			dao.closeAll();

//			return;	//test
		}

		PersonalDataDriver driver = new PersonalDataDriver();
		driver.drive(specTime);

		log.info("====driver end====");
	}

	/**
     * 刪除路徑.
     *
     * @param fs
     * 			FileSystem
     * @param path
     *			spec path
     * @param recursive
     *			delete recursively
     * @return
     * @throws IOException
     */
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

}
