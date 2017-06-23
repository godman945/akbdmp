package com.pchome.hadoopdmp.mapreduce.job.categorylog;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;

@Component
public class querMongoDate {

	public static void main(String[] args) throws Exception {
//		Log log = LogFactory.getLog("MongoInsertClassUrl");
//		log.info("MongoInsertClassUrl");
		
		try {
			System.setProperty("spring.profiles.active", "prd");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			
			
			//用mongodb.prop撈正式機舊資料
			MongoOperations oldMongoOperationsQuery = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
			MongoTemplate oldMongoOperationsQueryMongoTemplate = (MongoTemplate) oldMongoOperationsQuery;
			oldMongoOperationsQueryMongoTemplate.getDb().setOptions(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			
//			Query query = new Query(new Criteria());
//			Query query = new Query(Criteria.where("update_date").gt(getDate("2017-05-01 00:00:00","yyyy-MM-dd HH:mm:ss")).and("update_date").lt(getDate("2017-05-01 23:59:59","yyyy-MM-dd HH:mm:ss")));
			
			Query query = new Query(Criteria.where("update_date").gt("2017-06-01 23:59:59"));//.and("create_date").gt(15)以後的
			
//			'{"_id.day":{"$lt":{"$date":"2014-01-19T23:00:00Z"}}}
//			
//			mongodbConfig.set("mongo.input.query", "{'field':'value'}");
//			'mongo.input.query={"_id":{"$gt":{"$date":1182470400000}}}'

			//this is equivalent to {_id : {$gt : ISODate("2007-06-21T20:00:00-0400")}} in the mongo shell
			
			
//			Criteria.where("age").lt(40).and("age").gt(10)
//
//			   Criteria.where("created").gte(DateUtils.getDate("2016-04-14 00:00:00", "yyyy-MM-dd HH:mm:ss"),
//            Criteria.where("created").lte(DateUtils.getDate("2016-04-14 23:59:59", DateUtils.DB_FORMAT_DATETIME))
            
			
			List<ClassCountMongoBean> oldUrlMongoBean = oldMongoOperationsQuery.find(query, ClassCountMongoBean.class);
			System.out.println("size: "+oldUrlMongoBean.size());
			
			for (ClassCountMongoBean classCountMongoBean : oldUrlMongoBean) {
				System.out.println("User Id: "+classCountMongoBean.getUser_id());
				System.out.println(classCountMongoBean.getUpdate_date());
				
			}
		    
		    
		    
			
//			//測試機
//			String host="192.168.1.37";
//			int port=27017;
//			String db="pcbappdev";
//			String userName="webuser";
//			String password="axw2mP1i";
//			
//			
//			//正式機mongo
//			//新的insert mongo 物件
//			MongoOperations mongoOperationsInsert = new MongoTemplate(new SimpleMongoDbFactory(new Mongo(host, port),db, new UserCredentials(userName, password)));
//			MongoTemplate newInsertmongoTemplate = (MongoTemplate)mongoOperationsInsert;
//			newInsertmongoTemplate.setWriteConcern(WriteConcern.SAFE);
//			newInsertmongoTemplate.getDb().setOptions(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
//			
//			//新的query mongo 物件
//			MongoOperations newQueryMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo(host, port), db, new UserCredentials(userName, password)));
//			MongoTemplate mongoTemplate = (MongoTemplate) newQueryMongoOperations;
//			mongoTemplate.getDb().setOptions(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);
			
			


			System.out.println("OK");
		} catch (Exception e) {
			System.out.println(e.getMessage());
//			log.error(e.getMessage());
		}

	}


	    public static Date getDate(String dateStr, String format) throws java.text.ParseException {
	        final DateFormat formatter = new SimpleDateFormat(format);
	        return formatter.parse(dateStr);
	    }
}
