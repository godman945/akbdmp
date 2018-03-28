package com.pchome.akbdmp.spring.config.bean.multipart;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Option;

@Configuration
public class MultipartConfig extends WebMvcConfigurerAdapter{
    
	protected static ObjectMapper mapper = null;
	
    @Bean
    public CommonsMultipartResolver multipartResolver(){
    	CommonsMultipartResolver resolver = new CommonsMultipartResolver();
        resolver.setDefaultEncoding("UTF-8");
        return resolver;
    }
    
    @Bean(name= "objectMapper")
    public ObjectMapper createObjectMapper(){
    	mapper = new ObjectMapper();
    	mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        return mapper;
    }
    
    @Bean(name= "jsonpathConfiguration")
    public com.jayway.jsonpath.Configuration jsonpathConfiguration(){
    	com.jayway.jsonpath.Configuration configuration = com.jayway.jsonpath.Configuration.defaultConfiguration();
    	configuration = configuration.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
    	return configuration;
    }
    
    @Bean(name= "sendKafkaMap")
    public HashMap<String,String> sendKafkaMap(){
    	return new HashMap<String,String>();
    }
    
    
}
