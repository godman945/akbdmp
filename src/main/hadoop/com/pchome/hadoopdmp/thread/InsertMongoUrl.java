package com.pchome.hadoopdmp.thread;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.pchome.hadoopdmp.data.mongo.pojo.AdLogClassMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;


public class InsertMongoUrl implements Runnable {
	
	Log log = LogFactory.getLog("MongoInsertClassUrl");

	private MongoOperations oldMongoOperationsQuery ;
	private int skip;
	private int limit;
	private MongoTemplate newInsertmongoTemplate;
	private MongoOperations newQueryMongoOperations;

	
	
	public InsertMongoUrl(MongoTemplate oldMongoOperationsQuery, int skip, int limit, MongoTemplate newInsertmongoTemplate, MongoTemplate newQueryMongoOperations) {
		this.oldMongoOperationsQuery = oldMongoOperationsQuery;
		this.skip=skip;
		this.limit=limit;
		this.newInsertmongoTemplate = newInsertmongoTemplate;
		this.newQueryMongoOperations = newQueryMongoOperations;
	}

	public void run() {
		try{
			
			Query query = new Query(new Criteria());
		    query.skip(skip).limit(limit);
		    List<ClassUrlMongoBean> oldUrlMongoBean = oldMongoOperationsQuery.find(query, ClassUrlMongoBean.class);// 批次撈
		    
			for (ClassUrlMongoBean oldUrlMongoBean2 : oldUrlMongoBean) {
//				 i=i+1;
//				 System.out.println(i);

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
		
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}
	
	
}
