package alex.test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;


public class TEST {
	static Log log = LogFactory.getLog(TEST.class);
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "prd");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		MongoOperations mongoOperations = (MongoOperations) ctx.getBean(MongoOperations.class);
		
		
		/**多執行緒*/
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
		
		Future<Integer> mongoThreadProcess1Result = null;
		MongoThreadProcess mongoThreadProcess1 = new MongoThreadProcess(mongoOperations,0,999);
		mongoThreadProcess1Result = executor.submit(mongoThreadProcess1);
		
		Future<Integer> mongoThreadProcess2Result = null;
		MongoThreadProcess mongoThreadProcess2 = new MongoThreadProcess(mongoOperations,1000,1999);
		mongoThreadProcess2Result = executor.submit(mongoThreadProcess2);
		
		Future<Integer> mongoThreadProcess3Result = null;
		MongoThreadProcess mongoThreadProcess3 = new MongoThreadProcess(mongoOperations,2000,2999);
		mongoThreadProcess3Result = executor.submit(mongoThreadProcess3);
		
		Future<Integer> mongoThreadProcess4Result = null;
		MongoThreadProcess mongoThreadProcess4 = new MongoThreadProcess(mongoOperations,3000,5000);
		mongoThreadProcess4Result = executor.submit(mongoThreadProcess4);
		
		
		boolean mongoThreadProcess1ResultFlag = true;
		boolean mongoThreadProcess2ResultFlag = true;
		boolean mongoThreadProcess3ResultFlag = true;
		boolean mongoThreadProcess4ResultFlag = true;
		
		int totalDelete = 0;
		while (mongoThreadProcess1ResultFlag || mongoThreadProcess2ResultFlag || mongoThreadProcess3ResultFlag || mongoThreadProcess4ResultFlag) {
			if (mongoThreadProcess1Result.isDone() && mongoThreadProcess1ResultFlag) {
				int result = mongoThreadProcess1Result.get();
				log.info("執行緒1完成 --->需要刪除筆數:"+result);
				totalDelete = totalDelete + result;
				mongoThreadProcess1ResultFlag = false;
			}
			
			if (mongoThreadProcess2Result.isDone() && mongoThreadProcess2ResultFlag) {
				int result = mongoThreadProcess2Result.get();
				log.info("執行緒2完成 --->需要刪除筆數:"+result);
				totalDelete = totalDelete + result;
				mongoThreadProcess2ResultFlag = false;
			}
			
			if (mongoThreadProcess3Result.isDone() && mongoThreadProcess3ResultFlag) {
				int result = mongoThreadProcess3Result.get();
				log.info("執行緒3完成 --->需要刪除筆數:"+result);
				totalDelete = totalDelete + result;
				mongoThreadProcess3ResultFlag = false;
			}
			
			if (mongoThreadProcess4Result.isDone() && mongoThreadProcess4ResultFlag) {
				int result = mongoThreadProcess4Result.get();
				log.info("執行緒4完成 --->需要刪除筆數:"+result);
				totalDelete = totalDelete + result;
				mongoThreadProcess4ResultFlag = false;
			}
			
		}
		
		log.info(">>>>>>>>>>>>>>>>>>>> all thread finish totalDelete:"+totalDelete);
		executor.shutdown();
	}

}
