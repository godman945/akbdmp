package alex.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;


public class TEST {
	static Log log = LogFactory.getLog(TEST.class);
	
	public static void main(String[] args) throws Exception {
//		System.setProperty("spring.profiles.active", "prd");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
//		MongoOperations mongoOperations = (MongoOperations) ctx.getBean(MongoOperations.class);
		 
		long time1, time2;
		time1 = System.currentTimeMillis();
		Mongo m = new Mongo("mongodb.mypchome.com.tw");  
		DB db = m.getDB("dmp");
		db.authenticate("webuser", "MonG0Dmp".toCharArray());  
		DBCollection dBCollection = db.getCollection("user_detail");
//		DBCollection dBCollection = db.getCollection("class_url");
		
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
//		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lt", "2017-04-24")));
		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lt", "2017-04-24")));
		obj.add(new BasicDBObject("user_info.type", "uuid"));
		andQuery.put("$and", obj);
//		andQuery.put("update_date", new BasicDBObject("$lte", "2017-04-18"));
		System.out.println(andQuery.toString());
		DBCursor cursor = dBCollection.find(andQuery);
		
		
		
		System.out.println(cursor.count());
		time2 = System.currentTimeMillis();
		log.info(" >>>>>>>> cost " + (time2-time1)/1000 + " sec");
		
//		while(cursor.hasNext()) {
//			DBObject a = cursor.next();
//			System.out.println(a.get("_date"));
////			if(dBCollection.findOne(a) != null){
////				System.out.println("delete:" +a.get("_id"));
////				dBCollection.remove(a);
////			}
//		}
		
//		{ "$and" : [ { "update_date" : { "$lt" : "2017-04-24"}} , { "user_info.type" : "uuid"}]}
//		69805
//		2018/04/24 10:45:12 [INFO ] [alex.test.TEST@main]main(51)  >>>>>>>> cost 425 sec
		
		
		
		
		
		
//		
//		/**多執行緒*/
//		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
//		
//		Future<Integer> mongoThreadProcess1Result = null;
//		MongoThreadProcess mongoThreadProcess1 = new MongoThreadProcess(0,9999999,cursor,dBCollection);
//		mongoThreadProcess1Result = executor.submit(mongoThreadProcess1);
//		
//		Future<Integer> mongoThreadProcess2Result = null;
//		MongoThreadProcess mongoThreadProcess2 = new MongoThreadProcess(1000000,1999999,cursor,dBCollection);
//		mongoThreadProcess2Result = executor.submit(mongoThreadProcess2);
//		
//		Future<Integer> mongoThreadProcess3Result = null;
//		MongoThreadProcess mongoThreadProcess3 = new MongoThreadProcess(2000000,2999999,cursor,dBCollection);
//		mongoThreadProcess3Result = executor.submit(mongoThreadProcess3);
//		
//		Future<Integer> mongoThreadProcess4Result = null;
//		MongoThreadProcess mongoThreadProcess4 = new MongoThreadProcess(3000000,3999999,cursor,dBCollection);
//		mongoThreadProcess4Result = executor.submit(mongoThreadProcess4);
//		
//		
//		boolean mongoThreadProcess1ResultFlag = true;
//		boolean mongoThreadProcess2ResultFlag = true;
//		boolean mongoThreadProcess3ResultFlag = true;
//		boolean mongoThreadProcess4ResultFlag = true;
//		
//		int totalDelete = 0;
//		while (mongoThreadProcess1ResultFlag || mongoThreadProcess2ResultFlag || mongoThreadProcess3ResultFlag || mongoThreadProcess4ResultFlag) {
//			if (mongoThreadProcess1Result.isDone() && mongoThreadProcess1ResultFlag) {
//				int result = mongoThreadProcess1Result.get();
//				log.info("thread-1 total finish --->need delete count:"+result);
//				totalDelete = totalDelete + result;
//				mongoThreadProcess1ResultFlag = false;
//			}
//			
//			if (mongoThreadProcess2Result.isDone() && mongoThreadProcess2ResultFlag) {
//				int result = mongoThreadProcess2Result.get();
//				log.info("thread-2 total finish --->need delete count:"+result);
//				totalDelete = totalDelete + result;
//				mongoThreadProcess2ResultFlag = false;
//			}
//			
//			if (mongoThreadProcess3Result.isDone() && mongoThreadProcess3ResultFlag) {
//				int result = mongoThreadProcess3Result.get();
//				log.info("thread-3 total finish --->need delete count:"+result);
//				totalDelete = totalDelete + result;
//				mongoThreadProcess3ResultFlag = false;
//			}
//			
//			if (mongoThreadProcess4Result.isDone() && mongoThreadProcess4ResultFlag) {
//				int result = mongoThreadProcess4Result.get();
//				log.info("thread-4 total finish --->need delete count:"+result);
//				totalDelete = totalDelete + result;
//				mongoThreadProcess4ResultFlag = false;
//			}
//			
//		}
//		
//		log.info(">>>>>>>>>>>>>>>>>>>> all thread finish totalDelete:"+totalDelete);
//		executor.shutdown();
	}

}
