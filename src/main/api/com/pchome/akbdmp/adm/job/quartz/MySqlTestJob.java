package com.pchome.akbdmp.adm.job.quartz;


import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import org.mortbay.log.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import com.pchome.akbdmp.mysql.db.service.dmp.IKdclStatisticsSourceService;
import com.pchome.akbdmp.mysql.db.service.dmp.KdclStatisticsSourceService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

public class MySqlTestJob {
	@Autowired
	IKdclStatisticsSourceService kdclStatisticsSourceService; 
	
	@Scheduled(fixedDelay = 60000)
	public void execute() {
		try{
			Log.info("kdclStatisticsSource size:"+kdclStatisticsSourceService.loadAll().size());
			
			
			Statement statement = null;
			ResultSet resultSet = null;
			String url = "jdbc:mysql://dmpadm.mypchome.com.tw:3306/dmp";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "webuser";
			String password = "7e5nL0H";
			Class.forName(jdbcDriver);  
			Connection conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
			String sql = "select * from kdcl_statistics_source";
			resultSet = statement.executeQuery(sql);
			
			Log.info(String.valueOf(resultSet.getFetchSize()));
			conn.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String args[]){
//		try{
//			System.setProperty("spring.profiles.active", "stg");
//			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
//			IKdclStatisticsSourceService KdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
//			System.out.println("ALEX:"+KdclStatisticsSourceService.loadAll().size());
//		}catch(Exception e){
//			e.printStackTrace();
//		}
	}
	
}
