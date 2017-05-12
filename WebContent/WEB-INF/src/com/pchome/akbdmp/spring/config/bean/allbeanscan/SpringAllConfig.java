package com.pchome.akbdmp.spring.config.bean.allbeanscan;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.annotation.EnableTransactionManagement;



@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages ="com.pchome.akbdmp.spring.config.bean,"
		+ "com.pchome.soft.depot.utils,"
		+ "com.pchome.akbdmp.api.data,"
		+ "com.pchome.akbdmp.api.call.*.controller,"
		+ "com.pchome.akbdmp.mongo.db.service,"
		+ "com.pchome.akbdmp.mongo.db.dao,"
		+ "com.pchome.akbdmp.cayley.db.service,"
		+ "com.pchome.akbdmp.cayley.db.dao,"
//		+ "com.pchome.pcbapi.cayley.db.service,"
//		+ "com.pchome.pcbapi.cayley.db.dao,"
//		+ "com.pchome.soft.depot.utils,"
//		+ "com.pchome.pcbapi.api.data.check,"
		+ "alex.test"
		)
@PropertySource({
	"classpath:config/prop/${spring.profiles.active}/mongo.properties",
	"classpath:config/prop/${spring.profiles.active}/redis.properties",
	"classpath:config/prop/${spring.profiles.active}/mail.properties",
	"classpath:config/prop/${spring.profiles.active}/cayley.properties",
	"classpath:config/prop/${spring.profiles.active}/facebookapi.properties",
	"classpath:config/prop/${spring.profiles.active}/googleapi.properties",
	})
public class SpringAllConfig {
	
	   public SpringAllConfig(){
		   
	   }

}
