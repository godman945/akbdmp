package com.pchome.akbdmp.spring.config.bean.property;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

import com.jayway.jsonpath.Option;

@Component
public class PropertySourcesConfig {

	final static Logger log = Logger.getLogger(PropertySourcesConfig.class);


	@Bean
	public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
		log.info(">>>>>> ================init PropertySourcesConfig======================");
		return new PropertySourcesPlaceholderConfigurer();
	}
	
	
	@Bean(name= "jsonpathConfiguration")
    public com.jayway.jsonpath.Configuration jsonpathConfiguration(){
    	com.jayway.jsonpath.Configuration configuration = com.jayway.jsonpath.Configuration.defaultConfiguration();
    	configuration = configuration.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
    	return configuration;
    }

}
