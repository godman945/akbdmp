package com.pchome.akbdmp.spring.webxml.init;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.util.Log4jConfigListener;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.akbdmp.spring.config.bean.aop.AopConfig;
import com.pchome.akbdmp.spring.config.bean.freemarker.FreeMarkerConfig;
import com.pchome.akbdmp.spring.config.bean.multipart.MultipartConfig;
import com.pchome.akbdmp.spring.config.bean.tiles.TilesConfig;
import com.pchome.akbdmp.spring.config.restful.restfulscan.DispatcherConfig;
import com.pchome.filter.session.DisableUrlSessionFilter;

@SuppressWarnings("deprecation")
public class SpringWebAppInitializer extends WebMvcConfigurerAdapter implements WebApplicationInitializer {

	public void onStartup(ServletContext servletContext) throws ServletException {
		//log4j setup
		//servletContext.setInitParameter("log4jConfigLocation", "classpath:config/log4j/log4j.xml");
		servletContext.setInitParameter("log4jRefreshInterval", "10000");
		servletContext.setInitParameter("log4jExposeWebAppRoot", "false");
		Log4jConfigListener log4jListener = new Log4jConfigListener();
		servletContext.addListener(log4jListener);
		
		// Create the 'root' Spring application context
		AnnotationConfigWebApplicationContext rootContext = new AnnotationConfigWebApplicationContext();
		rootContext.register(SpringAllConfig.class);
		// 自動掃描路徑
		rootContext.register(DispatcherConfig.class);
		// 註冊多檔上傳
		rootContext.register(MultipartConfig.class);
		//註冊AOP攔截器
        rootContext.register(AopConfig.class);
    	//註冊freemarker
        rootContext.register(FreeMarkerConfig.class);
     	//註冊tiles
        rootContext.register(TilesConfig.class);
        
		//頁面相關設定
		FilterRegistration.Dynamic filterRegistration = servletContext.addFilter("encodingFilter", CharacterEncodingFilter.class);
		filterRegistration.setInitParameter("encoding", String.valueOf(StandardCharsets.UTF_8));
		//字符過濾器
		filterRegistration.setInitParameter("forceEncoding", "true");
		//配置頁面暫存
		filterRegistration.addMappingForUrlPatterns(null, false, "*.html");
		//servlet是否支持非同步
		filterRegistration.setAsyncSupported(true);
		filterRegistration.addMappingForServletNames(null, true, "AkbDmp");
		//去除url後面的jsessionid
		FilterRegistration.Dynamic disableUrlSessionFilter = servletContext.addFilter("disableUrlSessionFilter",DisableUrlSessionFilter.class);
		disableUrlSessionFilter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "*.html");
		// rest setup
		ServletRegistration.Dynamic dispatcher = servletContext.addServlet("AkbDmp", new DispatcherServlet(rootContext));
		dispatcher.setLoadOnStartup(1);
		dispatcher.addMapping("/");
		//spring setup
		servletContext.addListener(new ContextLoaderListener(rootContext));
	
	}
}
