package alex.test;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

@Component
@Scope("prototype")
public class ThreadTestRun {

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	DateFormatUtil dateFormatUtil;
	
	@SuppressWarnings({"resource","rawtypes"})
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		ThreadTestRun ThreadTestRun = ctx.getBean(ThreadTestRun.class);
		String threadName = "";
		String [] namePool = {"alex","Nico","bessie","boris","tim","cool","dyl","park","kylin","hebe"};
		RedisTemplate redisTemplate = ThreadTestRun.redisTemplate;
		long time1,time2;
		time1 = System.currentTimeMillis();
		for (int i = 0; i < namePool.length; i++) {
			ExecutorService service = Executors.newFixedThreadPool(100);
			for (int j = 0; j < 100; j++) {
				threadName = "task" + j;
				service.execute(new PressureTestThreadWorker(redisTemplate,threadName,namePool[i]));
			}
			service.shutdown();
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		}
		time2 = System.currentTimeMillis();
		System.out.println(">>>>COST============花費:"+((double) time2 - time1) / 1000+"秒");
	}

}
