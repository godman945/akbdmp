package com.pchome.hadoopdmp.dao.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class BaseDAO {
    private static String host;
    private static int port;
    private static String dbname;
    private static String user;
    private static String password;
//   	private static Mongo mongo = null;
    private static MongoClient mongoClient;
   	private static DB db = null;
    protected Log log = LogFactory.getLog(this.getClass());

    static {
//    	Properties prop = new Properties();
//    	try {
//			prop.load( BaseDAO.class.getResourceAsStream("/config/hadoop/mongodb.properties") );
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//
//    	host = prop.getProperty("mongodb.host");
//    	port = Integer.valueOf(prop.getProperty("mongodb.port"));
//    	dbname = prop.getProperty("mongodb.dbname");
//    	user = prop.getProperty("mongodb.user");
//    	password = prop.getProperty("mongodb.password");
    	
    	
    	
//    	//stg
//      	host ="mgodev.mypchome.com.tw";
//    	port = 27017;
//    	dbname = "dmp";
//    	user ="webuser";
//    	password ="axw2mP1i";

    	
//    	prd
        host = "mgocfgm.mypchome.com.tw";
        port = 27017;
        dbname = "dmp";
        user = "webuser";
        password = "MonG0Dmp";
        

    	//stg
//        host = "mgodev.mypchome.com.tw";
//        port = 27017;
//        dbname = "admin";
//        user = "webuser";
//        password = "axw2mP1i";

        try {
        	/* old */
//        	if(mongo == null){
//        		mongo = new Mongo(host, port);
//            	db = mongo.getDB(dbname);
//            	boolean auth = db.authenticate(user, password.toCharArray());
//            	db = mongo.getDB("dmp");	//QQ
//        	}

        	/* new */
        	MongoCredential credential = MongoCredential.createMongoCRCredential(user, dbname, password.toCharArray());
    		mongoClient = new MongoClient(new ServerAddress(host , port), Arrays.asList(credential));
    		db = mongoClient.getDB( "dmp" );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
    }

    protected int insert(List<DBObject> list, String collection) throws UnknownHostException {
        int count = 0;

        DBCollection dbCollection = db.getCollection(collection);
        count = dbCollection.insert(list).getN();

        return count;
    }

    protected int insert(DBObject dbObject, String collection) throws UnknownHostException {
    	int count = 0;

		DBCollection dbCollection = db.getCollection(collection);
		count = dbCollection.insert(dbObject).getN();

    	return count;
    }

    protected int delete(DBObject dbObject, String collection) throws UnknownHostException {
        int count = 0;
        DBCollection dbCollection = db.getCollection(collection);
        count = dbCollection.remove(dbObject).getN();
        return count;
    }

    protected void deleteAll(String collection) throws UnknownHostException {
    	DBCollection dbCollection = db.getCollection(collection);
    	dbCollection.drop();
    }


    protected int deleteByDate(DBObject query, String collection) throws UnknownHostException {

    	DBCollection dbCollection = db.getCollection(collection);
    	int result = dbCollection.remove(query).getN();

    	return result;
    }

    protected DBObject findByKeyword(String keyword, String collection) throws UnknownHostException {

        DBObject query = new BasicDBObject("keyword", keyword);
        DBCollection dbCollection = db.getCollection(collection);
        DBObject result = dbCollection.findOne(query);

        return result;
    }

    protected int findCountBySpecField(String field, String value, String collection) throws UnknownHostException {

        DBObject query = new BasicDBObject(field, value);
        DBCollection dbCollection = db.getCollection(collection);
        int result = dbCollection.find(query).count();

        return result;
    }

    protected int findCountBy2SpecField(String field1, String value1, String field2, String value2,String collection) throws UnknownHostException {

        DBObject query = new BasicDBObject(field1, value1);
        query.put(field2, value2);
        DBCollection dbCollection = db.getCollection(collection);
        int result = dbCollection.find(query).count();

        return result;
    }

    protected int findCountByDate(DBObject query, String collection) throws UnknownHostException {

    	DBCollection dbCollection = db.getCollection(collection);
    	int result = dbCollection.find(query).count();

    	return result;
    }

    protected List <DBObject> findByDate(int start, int limit, DBObject query, DBObject sort, String collection) throws UnknownHostException {

    	DBCollection dbCollection = db.getCollection(collection);
    	List <DBObject> results = dbCollection.find(query).skip(start).limit(limit).sort(sort).toArray();

    	return results;
    }

    protected DBCursor findByCustomQuery(DBObject dbObj, String collection) throws UnknownHostException {
    	DBObject query = dbObj;
        DBCollection dbCollection = db.getCollection(collection);
        DBCursor cursor = dbCollection.find(query);
        return cursor;
    }
    
    protected DBObject findOneByCustomQuery(DBObject dbObj, String collection) throws UnknownHostException {
    	DBObject query = dbObj;
        DBCollection dbCollection = db.getCollection(collection);
        DBObject rztDBObj = dbCollection.findOne(query);
        return rztDBObj;
    }

}