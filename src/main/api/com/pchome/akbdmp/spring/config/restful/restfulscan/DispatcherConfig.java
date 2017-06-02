package com.pchome.akbdmp.spring.config.restful.restfulscan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@EnableWebMvc
@Configuration
@ComponentScan(basePackages = "com.pchome.pcbapi.api.call.* *.controller")
public class DispatcherConfig extends WebMvcConfigurerAdapter{
	
	   public DispatcherConfig(){
		   
	   }
	   
	   @Override
	   public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
		   configurer.enable();
	   }

}
