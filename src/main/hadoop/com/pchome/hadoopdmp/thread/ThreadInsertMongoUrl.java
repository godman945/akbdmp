package com.pchome.hadoopdmp.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.stereotype.Component;

import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;

@Component
public class ThreadInsertMongoUrl {
	
	public static void main(String[] args) {
		Log log = LogFactory.getLog("MongoInsertClassUrl");
		
		log.info("ThreadInsertMongoUrl********** Start***********2017-06-21***************");
		
		try {
			System.setProperty("spring.profiles.active", "prd");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			
			//用mongodb.prop撈正式機舊資料
			MongoOperations oldMongoOperationsQuery = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
			MongoTemplate oldMongoOperationsQueryMongoTemplate = (MongoTemplate) oldMongoOperationsQuery;
			oldMongoOperationsQueryMongoTemplate.getDb().setOptions(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			
			
			//測試機
			String host="192.168.1.37";
			int port=27017;
			String db="pcbappdev";
			String userName="webuser";
			String password="axw2mP1i";
			
			
			
//			//正式機
//			String host="mongodb.mypchome.com.tw";
//			int port=27017;
//			String db="dmp";
//			String userName="webuser";
//			String password="MonG0Dmp";
			
			
			
			//正式機mongo
			//新的insert mongo 物件
			MongoOperations mongoOperationsInsert = new MongoTemplate(new SimpleMongoDbFactory(new Mongo(host, port),db, new UserCredentials(userName, password)));
			MongoTemplate newInsertmongoTemplate = (MongoTemplate)mongoOperationsInsert;
			newInsertmongoTemplate.setWriteConcern(WriteConcern.SAFE);
			newInsertmongoTemplate.getDb().setOptions(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			
			//新的query mongo 物件
			MongoOperations newQueryMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo(host, port), db, new UserCredentials(userName, password)));
			MongoTemplate mongoTemplate = (MongoTemplate) newQueryMongoOperations;
			mongoTemplate.getDb().setOptions(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);

		
//			int total = 5;
//			int bulk = 2;
//			int skip = 0;
//			int limit = 2;
			

			int total = 14081030;//14081030
			int bulk = 50000;
			int skip = 0;
			int limit = 50000;
			
			int threadNum = 10;
			ExecutorService executor = Executors.newFixedThreadPool(threadNum);
			int tc = threadNum;
			int taskName=0;
			while (total > 0) {
				taskName++;
				tc--;
				Runnable worker = (new InsertMongoUrl(oldMongoOperationsQueryMongoTemplate, skip, limit, newInsertmongoTemplate, mongoTemplate));
				executor.execute(worker);
				
				if (tc <= 0) {
					tc = threadNum;
					executor.shutdown();
					while (!executor.isTerminated()) {
					}
//					System.out.println("<----------------------------------Bulk Finished  threads-----------------> "+bulk);
//					log.info("<----------------------------------Finished  threads-----------------> "+ taskName);
					
					executor = Executors.newFixedThreadPool(threadNum);
				}
				
				total = total - bulk;
				skip = skip + bulk;
			}

			executor.shutdown();
			while (!executor.isTerminated()) {
			}
			
//			System.out.println("ThreadInsertMongoUrl********** Finished all threads***********");
			log.info("ThreadInsertMongoUrl********** Finished all threads***********");

		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}
	
}
