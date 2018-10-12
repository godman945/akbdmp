package com.pchome.hadoopdmp.spring.config.bean.mongodborg;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.DB;
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

		return dbObject;
	}
}
