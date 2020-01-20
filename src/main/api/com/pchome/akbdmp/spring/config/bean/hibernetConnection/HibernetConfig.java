package com.pchome.akbdmp.spring.config.bean.hibernetConnection;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Configuration
@EnableTransactionManagement
public class HibernetConfig {

	Log log = LogFactory.getLog(HibernetConfig.class);
	
	@Value("${jdbc.driverClass}")
	private String driverClass;
	
	
	@Value("${jdbc.driverUrl}")
	private String dbUrl;
	
	@Value("${jdbc.userName}")
	private String userName;
	
	@Value("${jdbc.userPassword}")
	private String userPassword;
	
	
	@Value("${c3p0.minPoolSize}")
	private int minPoolSize;
	
	
	@Value("${c3p0.maxPoolSize}")
	private int maxPoolSize;
	
	@Value("${c3p0.initialPoolSize}")
	private int initialPoolSize;
	
	@Value("${c3p0.maxIdleTime}")
	private int maxIdleTime;
	
	@Value("${c3p0.idleConnectionTestPeriod}")
	private int idleConnectionTestPeriod;
	
	@Value("${c3p0.acquireRetryAttempts}")
	private int acquireRetryAttempts;
	
	@Bean
	public DataSource restDataSource() {
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		try {

			dataSource.setDriverClass(driverClass);
			dataSource.setJdbcUrl(dbUrl);
			dataSource.setUser(userName);
			dataSource.setPassword(userPassword);
			dataSource.setMinPoolSize(minPoolSize);
			dataSource.setMaxPoolSize(maxPoolSize);
			dataSource.setInitialPoolSize(initialPoolSize);
			dataSource.setMaxIdleTime(maxIdleTime);
			dataSource.setIdleConnectionTestPeriod(idleConnectionTestPeriod);
			dataSource.setAcquireRetryAttempts(acquireRetryAttempts);

			
			log.info(dbUrl);
			log.info(driverClass);
			log.info(userName);
			log.info(userPassword);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dataSource;
	}

	public Properties hibernateProperties() {
		Properties properties = new Properties();
		properties.put("hibernate.dialect", "org.hibernate.dialect.MySQLDialect");
		properties.put("hibernate.show_sql", "false");
		properties.put("hibernate.format_sql", "false");
		properties.put("hibernate.connection.characterEncoding", "utf-8");
		properties.put("hibernate.use_sql_comments", "false");
		return properties;
	}

	@Bean
	public LocalSessionFactoryBean sessionFactory() {

		LocalSessionFactoryBean sessionFactory = new LocalSessionFactoryBean();
		try {                                                                                                     
			sessionFactory.setDataSource(restDataSource());
			sessionFactory.setPackagesToScan(new String[] { "com.pchome.akbdmp.data.mysql.pojo" });
			sessionFactory.setHibernateProperties(hibernateProperties());
			sessionFactory.afterPropertiesSet();
			return sessionFactory;
		} catch (Exception e) {
			System.err.println("Initial SessionFactory creation failed.");
			throw new RuntimeException(e);
		}

	}

	@Bean
	@Autowired
	public HibernateTransactionManager transactionManager(SessionFactory sessionFactory) {
		HibernateTransactionManager txManager = new HibernateTransactionManager();
		txManager.setSessionFactory(sessionFactory);
		return txManager;
	}

	@Bean
	public PersistenceExceptionTranslationPostProcessor exceptionTranslation() {
		return new PersistenceExceptionTranslationPostProcessor();
	}

}
