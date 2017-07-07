package com.pchome.hadoopdmp.mapreduce.job.categorylog;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.pchome.hadoopdmp.data.mongo.pojo.AdLogClassMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;

@Component
public class PageMethodMongoInsertClassUrl {

	public static void main(String[] args) throws Exception {
		Log log = LogFactory.getLog("MongoInsertClassUrl");
		log.info("MongoInsertClassUrl");
		
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			
			//正式機舊資料
			MongoOperations oldMongoOperationsQuery = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();

//			//新的insert mongo 物件
//			MongoOperations mongoOperationsInsert = new MongoTemplate(new SimpleMongoDbFactory(new Mongo("192.168.1.37", 27017), "pcbappdev", new UserCredentials("webuser", "axw2mP1i")));
//			MongoTemplate newInsertmongoTemplate = (MongoTemplate)mongoOperationsInsert;
//			newInsertmongoTemplate.setWriteConcern(WriteConcern.SAFE);
//			
//			//新的query mongo 物件
//			MongoOperations newQueryMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo("192.168.1.37", 27017), "pcbappdev", new UserCredentials("webuser", "axw2mP1i")));

			
			//新的insert mongo 物件
			MongoOperations mongoOperationsInsert = new MongoTemplate(new SimpleMongoDbFactory(new Mongo("mongodb.mypchome.com.tw", 27017), "dmp", new UserCredentials("webuser", "MonG0Dmp")));
			MongoTemplate newInsertmongoTemplate = (MongoTemplate)mongoOperationsInsert;
			newInsertmongoTemplate.setWriteConcern(WriteConcern.SAFE);
			
			//新的query mongo 物件
			MongoOperations newQueryMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo("mongodb.mypchome.com.tw", 27017), "dmp", new UserCredentials("webuser", "MonG0Dmp")));

			
			int total = 14082391;
			int bulk = 10000;
			int page = 0;

			while (total > 0) {
				
				Query query = new Query(new Criteria());
				query.with(new Sort(Sort.Direction.DESC, "_id"));
				query.with(new PageRequest(page, bulk));
				
			    List<ClassUrlMongoBean> oldUrlMongoBean = oldMongoOperationsQuery.find(query, ClassUrlMongoBean.class);// 用Page撈
			    
				for (ClassUrlMongoBean oldUrlMongoBean2 : oldUrlMongoBean) {
//					 i=i+1;
//					 System.out.println(i);

					//撈ad_log_class有無資料
					AdLogClassMongoBean newAdLogUrlMongoBeanQuery = null;
					Query queryNew = new Query(Criteria.where("url").is(oldUrlMongoBean2.getUrl()));
					newAdLogUrlMongoBeanQuery = newQueryMongoOperations.findOne(queryNew, AdLogClassMongoBean.class);

					if (newAdLogUrlMongoBeanQuery == null) {
						AdLogClassMongoBean newAdLogClassBeanCreate = new AdLogClassMongoBean();
						newAdLogClassBeanCreate.setAd_class(oldUrlMongoBean2.getAd_class());
						newAdLogClassBeanCreate.setStatus(oldUrlMongoBean2.getStatus());
						newAdLogClassBeanCreate.setUrl(oldUrlMongoBean2.getUrl());
						newAdLogClassBeanCreate.setCreate_date(oldUrlMongoBean2.getCreate_date());
						newAdLogClassBeanCreate.setUpdate_date(oldUrlMongoBean2.getUpdate_date());
						
						newInsertmongoTemplate.save(newAdLogClassBeanCreate);
						
					}else{
						
						if(newAdLogUrlMongoBeanQuery.getStatus().equals("0") && (oldUrlMongoBean2.getAd_class().matches("\\d{16}")) && (oldUrlMongoBean2.getStatus().equals("1"))){
							newAdLogUrlMongoBeanQuery.setAd_class(oldUrlMongoBean2.getAd_class());
							newAdLogUrlMongoBeanQuery.setStatus("1");
							newAdLogUrlMongoBeanQuery.setUpdate_date(oldUrlMongoBean2.getUpdate_date());
							
							newInsertmongoTemplate.save(newAdLogUrlMongoBeanQuery);
						}
						
					}
					
					
				}
				
				total = total - bulk;
				page = page + 1;
			}


//			Query query = Query.query(new Criteria());
//			query.skip(0);
//			query.limit(limit);
//			List<ClassUrlMongoBean> classUrlMongoBean = mongoOperations.find(query, ClassUrlMongoBean.class);// 批次撈
			
			
//			Query query = new Query();
//			query.addCriteria(Criteria.where("url").regex("ruten"));

//			query.with(new Sort(Direction.ASC, "_id"));
			
			
			// 撈mongo批次
			// Query query = Query.query(new Criteria());
			// query.skip(0);
			// query.limit(5);
			// List<ClassUrlMongoBean> classUrlMongoBean =
			// mongoOperations.find(query,ClassUrlMongoBean.class);//批次撈
			// for (ClassUrlMongoBean classUrlMongoBean2 : classUrlMongoBean) {
			// System.out.println(classUrlMongoBean2.get_id());
			// }

			// 撈mongo全部
			// List<ClassUrlMongoBean> classUrlMongoBean =
			// mongoOperations.findAll(ClassUrlMongoBean.class);

			// System.out.println("size: "+classUrlMongoBean.size());

			// //寫mongo table
			// MongoOperations mongoOperationsInsert = new MongoTemplate(new
			// SimpleMongoDbFactory(new Mongo("192.168.1.37",27017),"pcbappdev",
			// new UserCredentials("webuser","axw2mP1i")));
			// String adClass="TEST";
			// Date date = new Date();
			// ClassUrlMongoBeanTEST classUrlMongoBeanCreate = new
			// ClassUrlMongoBeanTEST();
			// classUrlMongoBeanCreate.setAd_class(adClass);
			// classUrlMongoBeanCreate.setStatus(adClass.matches("\\d{16}") ?
			// "1" : "0");
			// classUrlMongoBeanCreate.setUrl(adClass);
			// classUrlMongoBeanCreate.setCreate_date(date);
			// classUrlMongoBeanCreate.setUpdate_dateDate(date);
			// mongoOperationsInsert.save(classUrlMongoBeanCreate);

			System.out.println("OK");
		} catch (Exception e) {
			log.error(e.getMessage());
		}

	}
}
