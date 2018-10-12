package com.pchome.hadoopdmp.spring.config.bean.allbeanscan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages =  
	"com.pchome.hadoopdmp.mapreduce.category,"
	+"com.pchome.hadoopdmp.mapreduce.crawlbreadcrumb,"
	+"com.pchome.akbdmp.spring.config.bean.property,"
	+"com.pchome.hadoop.category.job,"
//	+"com.pchome.hadoopdmp.dao.sql,"
	+"com.pchome.hadoopdmp.mapreduce.job.categorylog,"
	+"com.pchome.hadoopdmp.mapreduce.job.personallog,"
	+"com.pchome.hadoopdmp.mapreduce.job.dedupip,"	
	+"com.pchome.hadoopdmp.spring.config.bean,"
//	+"com.pchome.hadoopdmp.mongo,"
//	+"com.pchome.hadoopdmp.thread,"
//	+"com.pchome.hadoopdmp.mysql.db,"
	+"com.pchome.soft.util,"
	+"com.pchome.hadoopdmp.spring.config.bean,"
//	+"test.bessie,"
//	+"com.pchome.hadoopdmp.mapreduce.job.RawData.TEST,"
//	+"com.pchome.hadoopdmp.mapreduce.job.Geoip2,"
	+"com.pchome.hadoopdmp.mapreduce.job.dmplog,"
//	+"com.pchome.hadoopdmp.mapreduce.job.combinerTest,"
	+"com.pchome.hadoopdmp.mapreduce.job.thirdcategorylog,"
	+"com.pchome.hadoopdmp.dao,"
	
)
@PropertySource({ 
	"classpath:config/hadoop/prop/${spring.profiles.active}/hdfs.properties",
	"classpath:config/hadoop/prop/${spring.profiles.active}/merge.properties",
	"classpath:config/hadoop/prop/${spring.profiles.active}/mongodb.properties",
	"classpath:config/hadoop/prop/${spring.profiles.active}/path.properties",
	"classpath:config/hadoop/prop/${spring.profiles.active}/redis.properties",
	"classpath:config/hadoop/prop/${spring.profiles.active}/kafka.properties"})
public class SpringAllHadoopConfig extends WebMvcConfigurerAdapter {

}
