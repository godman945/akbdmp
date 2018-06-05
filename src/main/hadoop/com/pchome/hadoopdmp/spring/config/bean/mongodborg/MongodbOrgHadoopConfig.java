package com.pchome.hadoopdmp.spring.config.bean.mongodborg;

import java.net.UnknownHostException;
import java.util.Arrays;

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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

@Configuration
public class MongodbOrgHadoopConfig {
	
    private static MongoClient mongoClient;
   	private static DB dbObject = null;

	@Value("${mongodb.user}")
	private String user;

	@Value("${mongodb.password}")
	private String password;

	@Value("${mongodb.host}")
	private String host;

	@Value("${mongodb.port}")
	private int port;

	@Value("${mongodb.dbname}")
	private String db;

	@Bean(name = "mongoOrgOperations")
	public DB mongoProducer() throws Exception {

		MongoCredential credential = MongoCredential.createMongoCRCredential(user, db, password.toCharArray());
		mongoClient = new MongoClient(new ServerAddress(host, port), Arrays.asList(credential));
		dbObject = mongoClient.getDB("dmp");

		// MongoOperations mongoOperations = new
		// MongoTemplate(mongoDbFactory());
		return dbObject;
	}

	protected DBObject findOneByCustomQuery(DBObject dbObj, String collection) throws UnknownHostException {
		DBObject query = dbObj;
		DBCollection dbCollection = dbObject.getCollection(collection);
		DBObject rztDBObj = dbCollection.findOne(query);
		return rztDBObj;
	}

	
	
//	 @SuppressWarnings("deprecation")
//	 public MongoDbFactory mongoDbFactory() throws Exception {
//	 UserCredentials userCredentials = new UserCredentials(user,password);
//	 return new SimpleMongoDbFactory(new Mongo(host,port),db,
//	 userCredentials);
//	 }
//
//	@Bean(name = "findAndModifyOptions")
//	public FindAndModifyOptions findAndModifyOptions() throws Exception {
//		FindAndModifyOptions options = new FindAndModifyOptions();
//		options.returnNew(true);
//		return options;
//	}
//
//	@Bean(name = "objectId")
//	public ObjectId objectId() throws Exception {
//		ObjectId objectId = new ObjectId();
//		return objectId;
//	}
}
