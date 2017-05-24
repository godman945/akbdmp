package alex.test;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.ClusterOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

@Component
public class RedisPressure {

	Log log = LogFactory.getLog(RedisPressure.class);
	
	

	@Autowired
	RedisTemplate<String, Object> redisTemplate;
	
	@Autowired
	DateFormatUtil dateFormatUtil;
	
	private void redisTest() throws Exception{
		
		System.out.println(redisTemplate.opsForSet().members("Nico"));
		
//		ClusterOperations<String, Object>  f = redisTemplate.opsForCluster();
		
//		redisTemplate.opsForValue().set("nico", "444");
		
//		System.out.println(redisTemplate.opsForValue().get("nico"));
//		redisTemplate
		
//		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
////		System.out.println(opsForSet.members("test1").size());
////		System.out.println(opsForSet.members("alex").size());
////		System.out.println(redisTemplate.opsForValue().get("alex"));
//		
////		redisTemplate.opsForValue().set("TEST", "G");
//////		System.out.println(redisTemplate.opsForValue().get("TEST"));
////		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
//////		opsForSet.add("test2", "0001");
////		System.out.println(redisTemplate.opsForValue().get("TEST"));
////		redisTemplate.opsForValue().set("T101", "02");
////		redisTemplate.opsForValue().set("TEST2", "A");
//		
//		
////		redisTemplate.delete("TEST");
//		
//		
//		System.out.println(redisTemplate.opsForValue().get("544544"));
		
		
		
		
		
//		System.out.println(redisTemplate.hasKey("alex"));
//		opsForSet.add("test1", "alex_test01");
//		System.out.println(opsForSet.members("alex"));
		
//		System.out.println(opsForSet.members("test1").toString());
//		opsForSet.add("test1", "alex");
//		for (Object obj : opsForSet.members("test1")) {
//			System.out.println(obj);
//		}
//		
//		ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
//		opsForSet.add("AA", "ZZZZZZZZZZZzz");
		
//		log.info(opsForSet.members("nico").size());
//		Date date = new Date(); 
//		System.out.println("開始時間:"+dateFormatUtil.getDateTemplate2().format(date));
//		ForkJoinPool forkJoinPool = new ForkJoinPool();
//		Future<Integer> future = forkJoinPool.submit(new ForkJoinProcess(0,10000000,opsForSet)); 
//		System.out.println("最後完成的size=" + future.get());  
//		forkJoinPool.shutdown();
//		Date date2 = new Date(); 
//		System.out.println("結束時間:"+dateFormatUtil.getDateTemplate2().format(date2));
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		RedisPressure redisPressure = (RedisPressure) ctx.getBean(RedisPressure.class);
		redisPressure.redisTest();
	}
}
