package com.pchome.hadoopdmp.spring.config.bean.mongodb;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.Mongo;

@Configuration
public class MongodbHadoopConfig {
	
	@Value("${mongodb.user}")
	private  String user;
	
	@Value("${mongodb.password}")
	private  String password;
	
	@Value("${mongodb.host}")
	private  String host;
	
	@Value("${mongodb.port}")
	private  int port;
	
	@Value("${mongodb.dbname}")
	private  String db;
	
	@Bean(name = "mongoOperations")
	public  MongoOperations  mongoProducer() throws Exception {
		MongoOperations mongoOperations = new MongoTemplate(mongoDbFactory());
		return mongoOperations;
	}
	
	@SuppressWarnings("deprecation")
	public  MongoDbFactory mongoDbFactory() throws Exception {
		UserCredentials userCredentials = new UserCredentials(user,password);
		return new SimpleMongoDbFactory(new Mongo(host,port),db, userCredentials);
	}

	@Bean(name = "findAndModifyOptions")
	public  FindAndModifyOptions findAndModifyOptions() throws Exception {
		FindAndModifyOptions options = new FindAndModifyOptions();
		options.returnNew(true);
		return options;
	}
	
	@Bean(name = "objectId")
	public ObjectId objectId() throws Exception {
		ObjectId objectId = new ObjectId();
		return objectId;
	}
}
