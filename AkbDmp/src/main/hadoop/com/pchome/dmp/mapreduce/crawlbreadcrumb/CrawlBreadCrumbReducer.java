//package com.pchome.dmp.mapreduce.crawlbreadcrumb;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Date;
//import java.util.List;
//import java.util.Properties;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import com.mongodb.BasicDBObject;
//import com.mongodb.DB;
//import com.mongodb.DBCollection;
//import com.mongodb.DBObject;
//import com.mongodb.MongoClient;
//import com.mongodb.MongoCredential;
//import com.mongodb.ServerAddress;
//import com.pchome.dmp.dao.mongodb.BaseDAO;
//
//public class CrawlBreadCrumbReducer extends Reducer<Text, Text, Text, Text> {
//
//	private Log log = LogFactory.getLog(this.getClass());
//
//	private String host;
//    private int port;
//    private String dbname;
//    private String user;
//    private String password;
//
//    private List<DBObject> list = new ArrayList<DBObject>();
//    private MongoClient mongoClient;
//    private DBCollection coll;
//
//    @Override
//    public void setup(Context context) {
//
//    	try{
//	    	Properties prop = new Properties();
//			prop.load( BaseDAO.class.getResourceAsStream("/config/prop/mongodb.properties") );
//
//			host = prop.getProperty("mongodb.host");
//	    	port = Integer.valueOf(prop.getProperty("mongodb.port"));
//	    	dbname = prop.getProperty("mongodb.dbname");
//	    	user = prop.getProperty("mongodb.user");
//	    	password = prop.getProperty("mongodb.password");
//
//	    	MongoCredential credential = MongoCredential.createMongoCRCredential(user, dbname, password.toCharArray());		//.createCredential(userName, database, password);
//			mongoClient = new MongoClient(new ServerAddress(host , port), Arrays.asList(credential));
//
//			DB db = mongoClient.getDB( "dmp" );
//			coll = db.getCollection("class_url");
//
//    	} catch(Exception e) {
//    		log.error(e.getMessage());
//    	}
//
//    }
//
//	@Override
//    public void reduce(Text key, Iterable<Text> value, Context context) {
//
////		String database = "admin";
////		String userName = "webuser";
////		String password = "axw2mP1i";
//
//		try {
////			Properties prop = new Properties();
////			prop.load( BaseDAO.class.getResourceAsStream("/config/prop/mongodb.properties") );
////
////			host = prop.getProperty("mongodb.host");
////	    	port = Integer.valueOf(prop.getProperty("mongodb.port"));
////	    	dbname = prop.getProperty("mongodb.dbname");
////	    	user = prop.getProperty("mongodb.user");
////	    	password = prop.getProperty("mongodb.password");
////
////	    	MongoCredential credential = MongoCredential.createMongoCRCredential(user, dbname, password.toCharArray());		//.createCredential(userName, database, password);
////			MongoClient mongoClient = new MongoClient(new ServerAddress(host , port), Arrays.asList(credential));
////
////			//old
//////			MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password.toCharArray());		//.createCredential(userName, database, password);
//////			MongoClient mongoClient = new MongoClient(new ServerAddress("mgodev.mypchome.com.tw" , 27017), Arrays.asList(credential));
////
////			DB db = mongoClient.getDB( "dmp" );
////			DBCollection coll = db.getCollection("class_url");
//
//			//older
////			List<DBObject> list = new ArrayList<DBObject>();
//			BasicDBObject doc;
//
//			log.info("key:" + key.toString());
//
//			for(Text url:value) {
//				doc = new BasicDBObject("url", url.toString())
//						.append("status", (key.toString().matches("\\d{16}")?"1":"0") )		//(0:未分類  1:已分類  2:跳過)
//						.append("ad_class", (key.toString().matches("\\d{16}")?key.toString():"") ).append("update_date", new Date()).append("create_date", new Date());
//				log.info("url(value):" + url.toString());
//				log.info("doc:" + doc.toString());
//				list.add( doc );
//			}
//
////			coll.insert(list);	//older
////			mongoClient.close();	//older
//
////			for(String str: listClassed) {
////				doc = new BasicDBObject("url", str)
////						.append("status", "1").append("ad_class", "CateA").append("update_date", new Date()).append("create_date", new Date());
////				System.out.println("insert result:" + coll.insert(doc) );
////			}
//
////			for(Text str:value) {
////				context.write( key , str );
////			}
//		} catch (Exception e) {
//			log.error("error" + e);
//		}
//
//	}
//
//	@Override
//    public void cleanup(Context context) {
//
//    	try {
//    		coll.insert(list);
//    		mongoClient.close();
//    	} catch (Exception e) {
//			log.error(e.getMessage());
//		}
//
//    }
//
//}
