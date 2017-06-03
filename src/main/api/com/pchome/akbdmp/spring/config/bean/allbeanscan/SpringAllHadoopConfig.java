package com.pchome.akbdmp.spring.config.bean.allbeanscan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages =  
	"com.pchome.dmp.mapreduce.category,"
	+"com.pchome.dmp.mapreduce.crawlbreadcrumb,"
	+"com.pchome.akbdmp.spring.config.bean.property,"
	+"com.pchome.hadoop.category.job,"
	+"com.pchome.dmp.dao.sql"
)
@PropertySource({ 
	"classpath:config/hadoop/hdfs.properties",
	"classpath:config/hadoop/merge.properties",
	"classpath:config/hadoop/mongodb.properties",
	"classpath:config/hadoop/path.properties",
	"classpath:config/hadoop/jdbc.properties",
	"classpath:config/hadoop/kafka.properties"})
public class SpringAllHadoopConfig extends WebMvcConfigurerAdapter {

}
