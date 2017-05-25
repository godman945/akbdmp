package alex.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

@Component
@Scope("prototype")
public class ThreadTestRun {

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	public void excute() throws Exception {

		//// SetOperations<String, Object> opsForSet =
		//// redisTemplate.opsForSet();
		//// System.out.println(opsForSet.members("nico").size());
		//// opsForSet.add("alex", "CC");
		//
		// int threadNum = 1000;
		// int total = 100000;
		////
		// ExecutorService executor = Executors.newFixedThreadPool(threadNum);
		// int tc = threadNum;
		// for (int i = 0; i < total; i++) {
		// tc--;
		// Runnable worker = new PressureTestThreadWorker(opsForSet,i);
		// executor.execute(worker);
		// if(tc <= 0){
		// tc= threadNum;
		// executor.shutdown();
		// while (!executor.isTerminated()) {
		// }
		// System.out.println("<----------------------------------Finished
		//// threads----------------->"+i);
		// executor = Executors.newFixedThreadPool(threadNum);
		// }
		// }
		//
		// executor.shutdown();
		// while (!executor.isTerminated()) {
		// }
		// System.out.println("Finished all threads");
	}

	@SuppressWarnings({"resource","rawtypes"})
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		ThreadTestRun ThreadTestRun = ctx.getBean(ThreadTestRun.class);
		String threadName = "";
		String [] namePool = {"alex","Nico","bessie","boris","tim","cool","dyl","park","kylin","hebe"};
		for (int i = 0; i < 10; i++) {
			ExecutorService service = Executors.newFixedThreadPool(10);
			RedisTemplate redisTemplate = ThreadTestRun.redisTemplate;
			for (int j = 0; j < 20; j++) {
				threadName = "task" + j;
				service.execute(new PressureTestThreadWorker(redisTemplate,threadName,namePool[i]));
			}
			service.shutdown();
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			System.out.println(namePool[i] + "  thread complete");
		}
		System.out.println("all thread complete");
	}

}
