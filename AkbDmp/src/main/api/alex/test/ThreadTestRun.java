package alex.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

@Component
public class ThreadTestRun {

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	DateFormatUtil dateFormatUtil;

	@SuppressWarnings({ "resource", "rawtypes" })
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		ThreadTestRun ThreadTestRun = ctx.getBean(ThreadTestRun.class);
		String threadName = "";
		String[] namePool = { "alex", "Nico", "bessie", "boris", "tim", "cool", "dyl", "park", "kylin", "hebe" };
		RedisTemplate redisTemplate = ThreadTestRun.redisTemplate;
		int threadPoolDefault = 200;
		int threadPool = threadPoolDefault;
		ExecutorService service = null;
		service = Executors.newFixedThreadPool(threadPoolDefault);
		long time1, time2;
		time1 = System.currentTimeMillis();
		for (int i = 0; i < namePool.length; i++) {
			for (int j = 0; j < 200; j++) {
				threadPool--;
				threadName = "task" + j;
				service.execute(new PressureTestThreadWorker(redisTemplate, threadName, namePool[i]));
			
				if (threadPool <= 0) {
					threadPool = threadPoolDefault;
					service.shutdown();
					while (!service.isTerminated()) {
					}
					System.out.println(threadName + "============= 批次處理完畢");
					service = Executors.newFixedThreadPool(threadPoolDefault);
				}
			}
		}
		service.shutdown();
	  	while (!service.isTerminated()) {
		}
		time2 = System.currentTimeMillis();
		System.out.println(">>>>COST============花費:" + ((double) time2 - time1) / 1000 + "秒");
	}

}
