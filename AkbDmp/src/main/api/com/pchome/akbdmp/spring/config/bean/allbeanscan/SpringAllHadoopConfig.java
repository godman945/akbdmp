package com.pchome.akbdmp.spring.config.bean.allbeanscan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages =  
	 "com.pchome.dmp.mapreduce.crawlbreadcrumb,"
	+ "com.pchome.akbdmp.spring.config.bean.property"
)
@PropertySource({ 
	"classpath:config/hadoop/hdfs.properties",
	"classpath:config/hadoop/path.properties" })
public class SpringAllHadoopConfig extends WebMvcConfigurerAdapter {

}
