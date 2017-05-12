package com.pchome.akbdmp.spring.config.webxml.init;

import java.nio.charset.StandardCharsets;

import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.util.Log4jConfigListener;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.akbdmp.spring.config.bean.aop.AopConfig;
import com.pchome.akbdmp.spring.config.bean.multipart.MultipartConfig;
import com.pchome.akbdmp.spring.config.restful.restfulscan.DispatcherConfig;

@SuppressWarnings("deprecation")
public class SpringWebAppInitializer implements WebApplicationInitializer {
	 

	@Override
    public void onStartup(ServletContext servletContext) throws ServletException {
		//設定log4j
        servletContext.setInitParameter( "log4jConfigLocation" , "classpath:config/log4j/log4j.xml" );
		servletContext.setInitParameter( "log4jRefreshInterval" , "10000" );
		servletContext.setInitParameter( "log4jExposeWebAppRoot", "false" );
		Log4jConfigListener log4jListener = new Log4jConfigListener();
		servletContext.addListener(log4jListener);
		
		//Manage the life cycle of the root application context 取得所請求資源的URL、設定與儲存屬性、應用程式初始參數，甚至動態設定Servlet實例
		AnnotationConfigWebApplicationContext rootContext = new AnnotationConfigWebApplicationContext();
		rootContext.register(SpringAllConfig.class);
		
		//自動掃描路徑
        rootContext.register(DispatcherConfig.class);
        //註冊多檔上傳
        rootContext.register(MultipartConfig.class);
        //註冊AOP攔截器
        rootContext.register(AopConfig.class);
        
		//頁面相關設定
		FilterRegistration.Dynamic filterRegistration = servletContext.addFilter("encodingFilter", CharacterEncodingFilter.class);
		filterRegistration.setInitParameter("encoding", String.valueOf(StandardCharsets.UTF_8));
		//字符過濾器
		filterRegistration.setInitParameter("forceEncoding", "true");
		//配置頁面暫存
		filterRegistration.addMappingForUrlPatterns(null, false, "/*");
		//servlet是否支持非同步
		filterRegistration.setAsyncSupported(true);
		
		filterRegistration.addMappingForServletNames(null, true, "AkbDmp");
        
        //註冊一個servlet
        ServletRegistration.Dynamic dispatcher = servletContext.addServlet("AkbDmp",new DispatcherServlet(rootContext));
		dispatcher.setLoadOnStartup(2);
		dispatcher.setAsyncSupported(true);
		dispatcher.addMapping("/");
		
		servletContext.addListener(new ContextLoaderListener(rootContext));
	}
}
