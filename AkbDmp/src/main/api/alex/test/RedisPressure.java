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
public class RedisPressure {

	Log log = LogFactory.getLog(RedisPressure.class);
	
	

	@Autowired
	RedisTemplate<String, Object> redisTemplate;
	
	@Autowired
	DateFormatUtil dateFormatUtil;
	
	private void redisTest() throws Exception{
		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		Date date = new Date(); 
		System.out.println("開始時間:"+dateFormatUtil.getDateTemplate2().format(date));
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		Future<Integer> future = forkJoinPool.submit(new ForkJoinProcess(0,1,opsForSet)); 
		System.out.println("最後完成的size=" + future.get());  
		forkJoinPool.shutdown();
		Date date2 = new Date(); 
		System.out.println("結束時間:"+dateFormatUtil.getDateTemplate2().format(date2));
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		RedisPressure redisPressure = (RedisPressure) ctx.getBean(RedisPressure.class);
		redisPressure.redisTest();
	}
}
