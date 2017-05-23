package alex.test;

import java.util.Date;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

@Component
public class TestRun3 {

	Log log = LogFactory.getLog(TestRun3.class);
	
	

	@Autowired
	RedisTemplate<String, Object> redisTemplate;
	
	@Autowired
	DateFormatUtil dateFormatUtil;
	
	private void redisTest() throws Exception{
		
		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		
//		System.out.println(opsForSet.members("nico").size());
//		
//		redisTemplate.delete("nico");
//		
//		
		Date date = new Date(); 
		System.out.println("開始時間:"+dateFormatUtil.getDateTemplate2().format(date));
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		Future<Integer> future = forkJoinPool.submit(new ForkJoinProcess(0,1000000,opsForSet)); 
		System.out.println("最後完成的size=" + future.get());  
		forkJoinPool.shutdown();
		Date date2 = new Date(); 
		System.out.println("結束時間:"+dateFormatUtil.getDateTemplate2().format(date2));
		
		
		
		
		
		
//		for (int i = 0; i < 1000000; i++) {
//			Random randData = new Random();
//			int no = randData.nextInt(29);
//			String testStr = data[no];	
//			System.out.println(i+":"+testStr);
//			opsForSet.add("nico", testStr);
//		}
		
		
		//1.
//		redisTemplate.opsForSet().add("alex", "A01");
//		 SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		 
		 
//		 String n01 = "N01";
//		 String n02 = "N02";
//		 String n03 = "N03";
//		 String n04 = "A01";
//		 
//		 opsForSet.add("nico", n01);
//		 opsForSet.add("nico", n02);
//		 opsForSet.add("nico", n03);
//		 opsForSet.add("nico", n04);
		 
//		 String a01 = "A01";
//		 String a02 = "A02";
//		 String a03 = "A03";
//		 
//		 opsForSet.add("alex", a01);
//		 opsForSet.add("alex", a02);
//		 opsForSet.add("alex", a03);
		 
		 
//		 System.out.println("nico:"+opsForSet.members("nico") +" size:"+ opsForSet.members("nico").size());
//		 System.out.println("alex:"+opsForSet.members("alex") +" size:"+ opsForSet.members("alex").size());
		 
		 
		 //1.差集
//		 System.out.println("nico,alex 差集: "+opsForSet.difference("nico", "alex")+" size:"+opsForSet.difference("nico", "alex").size());
//		 //2.交集
//		 System.out.println("nico,alex 交集: "+opsForSet.intersect("nico", "alex") +" size:"+opsForSet.intersect("nico", "alex").size());
//		 //3.聯集
//		 System.out.println("nico,alex 聯集: "+opsForSet.union("nico", "alex") +" size:"+opsForSet.union("nico", "alex").size());
//		 
//		 
//		 System.out.println("--------- START TEST- LIST -------------");
//		 ListOperations<String, Object> opsForList = redisTemplate.opsForList();
		 
		 //從右自左發佈消息
//		 opsForList.leftPushAll("bessie", "B01", "B02", "B03");
//		 System.out.println(opsForList.leftPop("bessie"));
		 
		 
//		 List<String> list = new ArrayList<>();
//		 list.add("B01");
//		 list.add("B02");
//		 list.add("B03");
//		 list.add("B04");
//		 redisTemplate.opsForValue().set("bessie", list);
		 
//		 ValueOperations<String, Object> opsForValue = redisTemplate.opsForValue();
//		 System.out.println(opsForValue.get("bessie"));
//		 
//		 
//		 
//		 
//		 HashOperations<String, Object,Object> opsForHash = redisTemplate.opsForHash();
//		 
		 
		 
		 
		 
		 
		 
//		 opsForList.rightPush("bessie",list); 
		 
		 
//		 Long size = opsForList.size("bessie"); 
//		 System.out.println(size);
//		 
//		 
//		 opsForList.leftPop("bessie")
		 
		 
		 
		 
		 
//		System.out.println(redisTemplate.opsForSet().pop("alex"));
		
		
		
//		redisTemplate.opsForSet().add("alex", "A1");
	
//		redisTemplate.opsForValue().set("alex", "123");
		
//		System.out.println(redisTemplate.opsForValue().get("alex"));
		
		
//		redisTemplate.delete("alex");
		//設置自增數字
////		 redisTemplate.opsForValue().increment(key, data);
//		
//		
//		Long a = redisTemplate.opsForValue().increment(key, 5);
//		System.out.println(a);
		
		//寫值
//		redisTemplate.opsForValue().set(key, value, timeOut, TimeUnit.SECONDS);
//		redisTemplate.opsForValue().set(key, value);
//		System.out.println(redisTemplate.getExpire(key));
//		redisTemplate.opsForValue().get(key);
		//取值
//		log.info(">>>>>> "+redisTemplate.opsForValue().get(key));
//		redisTemplate.opsForValue().set(key, value);
//		redisTemplate.expire(key, timeOut, TimeUnit.SECONDS);
//		redisTemplate.delete(key);
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		TestRun3 TestRun3 = (TestRun3) ctx.getBean(TestRun3.class);
		TestRun3.redisTest();
	}
}
