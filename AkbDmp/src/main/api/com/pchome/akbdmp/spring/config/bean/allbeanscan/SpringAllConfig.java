package com.pchome.akbdmp.spring.config.bean.allbeanscan;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;
import org.springframework.web.servlet.view.freemarker.FreeMarkerViewResolver;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages =  
	"com.pchome.akbdmp.spring.config.bean,"
	+ "com.pchome.akbdmp.api.call.*.controller,"
	+ "com.pchome.soft.depot.utils,"
	+ "com.pchome.akbdmp.api.data,"
	+ "alex.test,"
// + "com.pchome.soft.depot.utils,"
// + "com.pchome.akbdmp.api.data,"
// + "com.pchome.akbdmp.api.call.*.controller,"
// + "com.pchome.akbdmp.mongo.db.service,"
// + "com.pchome.akbdmp.mongo.db.dao,"
// + "com.pchome.akbdmp.cayley.db.service,"
// + "com.pchome.akbdmp.cayley.db.dao,"
// + "com.pchome.pcbapi.cayley.db.service,"
// + "com.pchome.pcbapi.cayley.db.dao,"
// + "com.pchome.soft.depot.utils,"
// + "com.pchome.pcbapi.api.data.check,"
)
@PropertySource({ 
	"classpath:config/prop/${spring.profiles.active}/mongo.properties",
	"classpath:config/prop/${spring.profiles.active}/redis.properties" })
public class SpringAllConfig extends WebMvcConfigurerAdapter {

	  @Override
	  public void addResourceHandlers(ResourceHandlerRegistry registry) {
		  registry.addResourceHandler("/resources/**").addResourceLocations("/resources/");
	  }
	 
	    @Override
	    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
	        configurer.enable();
	    }
	 
	    /*
	    @Bean
	    public InternalResourceViewResolver jspViewResolver() {
	        InternalResourceViewResolver bean = new InternalResourceViewResolver();
	        bean.setPrefix("/WEB-INF/views/");
	        bean.setSuffix(".jsp");
	        return bean;
	    }
	    */
	    
	    @Bean 
	    public FreeMarkerViewResolver freemarkerViewResolver() { 
	        FreeMarkerViewResolver resolver = new FreeMarkerViewResolver(); 
	        resolver.setCache(true); 
	        resolver.setPrefix(""); 
	        resolver.setSuffix(".ftl"); 
	        return resolver; 
	    }
	    
	    @Bean 
	    public FreeMarkerConfigurer freemarkerConfig() { 
	        FreeMarkerConfigurer freeMarkerConfigurer = new FreeMarkerConfigurer(); 
	        freeMarkerConfigurer.setTemplateLoaderPath("/WEB-INF/views/");
	        return freeMarkerConfigurer; 
	    }
	 
	    @Bean(name = "multipartResolver")
	    public CommonsMultipartResolver getMultipartResolver() {
	        return new CommonsMultipartResolver();
	    }
	 
	    @Bean(name = "messageSource")
	    public ReloadableResourceBundleMessageSource getMessageSource() {
	        ReloadableResourceBundleMessageSource resource = new ReloadableResourceBundleMessageSource();
	        resource.setBasename("classpath:messages");
	        resource.setDefaultEncoding("UTF-8");
	        return resource;
	    }
}
