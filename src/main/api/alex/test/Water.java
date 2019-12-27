package alex.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.io.IOUtils;
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
		
		
		
		
		if(true){
			
//			int A =3,B=2 ,C=1 ,D=-100;
//			int [] array = {A,B,C,D};
//	        int temp;
//	        for (int i = 0; i < array.length; i++) {
//	            for (int j = i+1; j < array.length; j++) {
//	                if (array[i] < array[j]) {
//	                    temp = array[i];
//	                    array[i] = array[j];
//	                    array[j] = temp;
//	                }
//	            }
//	        }
//		
//		int max = array[0];
//		int min = array[3];
//		
//		int first = (max - min);
//        int se = array[1] -  array[2];
//        int answ = (int)Math.pow(first, 2)+(int)Math.pow(se, 2);
//        System.out.println(answ);
//***********************************************************************			
//			int k = 3;
//			int l = 2;
//			int [] array = {6,1,4,6,3,2,7,4};
//			int k = 1;
//			int l = 3;
////			int [] array = {10,19,15};
//			
//			List<Integer> list = new ArrayList<>();
//			for (Object object : array) {
//				list.add((Integer) object);
//			}
//			
//			int cobtroller = 0;
//			for (int i=0; i<array.length;i++) {
//				int start = (k-1);
//				if(i >= start && cobtroller < k){
//					cobtroller = cobtroller + 1;
//					System.out.println("k:"+array[i]);
//				}
//			}
//			cobtroller = cobtroller + l;
//			System.out.println("cobtroller:"+cobtroller);
//			int end = 0;
//			for (int i=0; i<array.length;i++) {
//				if(i >= cobtroller && end < l){
//					System.out.println("l:"+array[i]);
//					end = end +1;
//				}
//			}
			
			
			Map data = new HashMap<>();
			for (int i = 0; i < 2000000; i++) {
				System.out.println(i);
				data.put(i, "alex"+i);
			}
			
			
//			int count = 0;
//			int index = 0;
//			List<Integer> controller = new ArrayList<Integer>();
//			boolean flag = true;
//			for (int i = 0; i < array.length; i++) {
//				if((i >= (k-1)) && i<(k+1)){
//					System.out.println(array[i]);
//					count = count + array[i];
//					index = i;
//					controller.add(i);
//				}
//			}
//			int con = 0;
//			if(k == (array.length-1)){
//				flag = false;
//			}
//			
//			System.out.println("index:"+index);
//			for (int i = 0; i < array.length; i++) {
//				if(i>= (index + l) &&  con <l){
//					con = con +1;
//					count = count + array[i];
//					System.out.println(array[i]);
//					
//					for (Integer integer : controller) {
//						if(integer == i){
//							flag = false;
//						}
//					}
//					if(flag){
//						controller.add(i);
//					}
//				}
//			}
//			if(!flag){
//				count = -1;
//			}
//			System.out.println("ans:"+count);
////			System.out.println(controller);
//			System.out.println("{'record_count':1416,'ad_class':'2671267200000000','record_date':'2018-06-21','data':{'classify':[{'memid_kdcl_log_personal_info_api':'null'},{'all_kdcl_log_personal_info':'N'},{'all_kdcl_log_class_ad_click':'null'},{'all_kdcl_log_class_24h_url':'null'},{'all_kdcl_log_class_ruten_url':'null'},{'all_kdcl_log_area_info':'Y'},{'all_kdcl_log_device_info':'N'},{'all_kdcl_log_time_info':'Y'}],'sex_info':{'source':'null','value':'null'},'device_phone_info':{'source':'user-agent','value':'OTHER'},'device_browser_info':{'source':'user-agent','value':'Robot/Spider'},'category_info':{'source':'null','value':'null'},'device_os_info':{'source':'user-agent','value':'UNKNOWN'},'age_info':{'source':'null','value':'null'},'time_info':{'source':'datetime','value':'16'},'area_country_info':{'source':'ip','value':'United States'},'device_info':{'source':'user-agent','value':'UNKNOWN'},'area_city_info':{'source':'ip','value':'Mountain View'}},'user_agent':'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)','org_source':'kdcl','date_time':'2018-05-22 16:06:53','url':'http://news.pchome.com.tw/living/ctv/20180510/video-52590680056417208009.html','key':{'uuid':'','memid':'alex_test_20180621_"+""+"'},'ip':'66.249.71.157'}");
			
			
//			String [] str = {"開車不喝酒，喝酒不開車"};
//			char[] chars = Arrays.asList(str[0]).toString().toCharArray();
//			Set<String> set = new HashSet<>();
//			for (char c : chars) {
//				
//				System.out.println(c);
//				set.add(String.valueOf(c));
//			}
//			
//			System.out.println(set);
//			
////			List a = Arrays.asList(str[0]);
////			System.out.println(a);
////			Set<String> set = new HashSet<>();
////			for (String string : str) {
////				System.out.println(string);
////				set.add(string);
////			}
////			System.out.println(set);
//			
//			
//			
			System.exit(0);
		}
		
		
//		System.setProperty("spring.profiles.active", "prd");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
//		MongoOperations mongoOperations = (MongoOperations) ctx.getBean(MongoOperations.class);
		 
//		long time1, time2;
//		time1 = System.currentTimeMillis();
//		Mongo m = new Mongo("mongodb.mypchome.com.tw");  
//		DB db = m.getDB("dmp");
//		db.authenticate("webuser", "MonG0Dmp".toCharArray());  
//		DBCollection dBCollection = db.getCollection("user_detail");
////		DBCollection dBCollection = db.getCollection("class_url");
//		BasicDBObject andQuery = new BasicDBObject();
//		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
////		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lt", "2017-04-24")));
//		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lt", "2017-06-14")));
//		obj.add(new BasicDBObject("user_info.type", "uuid"));
//		andQuery.put("$and", obj);
////		andQuery.put("update_date", new BasicDBObject("$lte", "2017-04-18"));
//		System.out.println(andQuery.toString());
//		DBCursor cursor = dBCollection.find(andQuery);
//		System.out.println(cursor.count());
//		time2 = System.currentTimeMillis();
//		log.info(" >>>>>>>> cost " + (time2-time1)/1000 + " sec");
		
		
		
		
		
		
		long time1, time2;
		time1 = System.currentTimeMillis();
		Mongo m = new Mongo("mongodb.mypchome.com.tw");  
		DB db = m.getDB("dmp");
		db.authenticate("webuser", "MonG0Dmp".toCharArray());
		
//		Mongo mongo;
//		mongo = new Mongo("192.168.1.37",27017);
//		DB db = mongo.getDB("dmp");
//		db.authenticate("webuser", "axw2mP1i".toCharArray());
//		
		
		DBCollection dBCollection = db.getCollection("user_detail");
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("user_info.type", "uuid"));
		obj.add(new BasicDBObject("update_date", new BasicDBObject("$lt", "2017-06-21")));
//		obj.add(new BasicDBObject("update_date", new BasicDBObject("$gt", "2018-06-13")));
		andQuery.put("$and", obj);
//		andQuery.put("update_date", new BasicDBObject("$lte", "2017-06-15"));
		System.out.println(andQuery.toString());
		DBCursor cursor = dBCollection.find(andQuery);
		System.out.println(cursor.count());
		time2 = System.currentTimeMillis();
//		136240
		
//		time2 = System.currentTimeMillis();
		log.info(" >>>>>>>> cost " + (time2-time1)/1000 + " sec");
		int i = 0;
		while(cursor.hasNext()) {
			DBObject a = cursor.next();
//			System.out.println(a.get("_date"));
			if(dBCollection.findOne(a) != null){
				i = i+1;
				System.out.println("process count:"+i+" "+a.get("_id"));
				dBCollection.remove(a);
			}
		}
		
//		{ "$and" : [ { "update_date" : { "$lt" : "2017-04-24"}} , { "user_info.type" : "uuid"}]}
//		69805
//		2018/04/24 10:45:12 [INFO ] [alex.test.TEST@main]main(51)  >>>>>>>> cost 425 sec
		
		
		
		
		
		
		
		/**多執行緒*/
//		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
//		
//		Future<Integer> mongoThreadProcess1Result = null;
//		MongoThreadProcess mongoThreadProcess1 = new MongoThreadProcess(0,50000,cursor,dBCollection);
//		mongoThreadProcess1Result = executor.submit(mongoThreadProcess1);
		
//		Future<Integer> mongoThreadProcess2Result = null;
//		MongoThreadProcess mongoThreadProcess2 = new MongoThreadProcess(10001,20000,cursor,dBCollection);
//		mongoThreadProcess2Result = executor.submit(mongoThreadProcess2);
		
//		Future<Integer> mongoThreadProcess3Result = null;
//		MongoThreadProcess mongoThreadProcess3 = new MongoThreadProcess(20001,30000,cursor,dBCollection);
//		mongoThreadProcess3Result = executor.submit(mongoThreadProcess3);
//		
//		Future<Integer> mongoThreadProcess4Result = null;
//		MongoThreadProcess mongoThreadProcess4 = new MongoThreadProcess(30001,40000,cursor,dBCollection);
//		mongoThreadProcess4Result = executor.submit(mongoThreadProcess4);
		
		
//		boolean mongoThreadProcess1ResultFlag = true;
//		boolean mongoThreadProcess2ResultFlag = true;
//		boolean mongoThreadProcess3ResultFlag = true;
//		boolean mongoThreadProcess4ResultFlag = true;
//		
//		int totalDelete = 0;
//		while (mongoThreadProcess1ResultFlag ) {
//			if (mongoThreadProcess1Result.isDone() && mongoThreadProcess1ResultFlag) {
//				int result = mongoThreadProcess1Result.get();
//				log.info("thread-1 total finish --->need delete count:"+result);
//				totalDelete = totalDelete + result;
//				mongoThreadProcess1ResultFlag = false;
//			}
////			
////			if (mongoThreadProcess2Result.isDone() && mongoThreadProcess2ResultFlag) {
////				int result = mongoThreadProcess2Result.get();
////				log.info("thread-2 total finish --->need delete count:"+result);
////				totalDelete = totalDelete + result;
////				mongoThreadProcess2ResultFlag = false;
////			}
////			
////			if (mongoThreadProcess3Result.isDone() && mongoThreadProcess3ResultFlag) {
////				int result = mongoThreadProcess3Result.get();
////				log.info("thread-3 total finish --->need delete count:"+result);
////				totalDelete = totalDelete + result;
////				mongoThreadProcess3ResultFlag = false;
////			}
////			
////			if (mongoThreadProcess4Result.isDone() && mongoThreadProcess4ResultFlag) {
////				int result = mongoThreadProcess4Result.get();
////				log.info("thread-4 total finish --->need delete count:"+result);
////				totalDelete = totalDelete + result;
////				mongoThreadProcess4ResultFlag = false;
////			}
////			
//		}
		
//		log.info(">>>>>>>>>>>>>>>>>>>> all thread finish totalDelete:"+totalDelete);
//		executor.shutdown();
	}

}
