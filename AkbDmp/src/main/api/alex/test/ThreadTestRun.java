package alex.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

	Log log = LogFactory.getLog(ThreadTestRun.class);

	public void test() {
		try {
			String threadName = "";
			String[] namePool = { "alex", "Nico", "bessie", "boris", "tim", "cool", "dyl", "park", "kylin", "hebe" };
			int threadPoolDefault = 150;
			int threadPool = threadPoolDefault;
			ExecutorService service = null;
			service = Executors.newFixedThreadPool(threadPoolDefault);
			long time1, time2;
			time1 = System.currentTimeMillis();
			for (int i = 0; i < 1; i++) {
				for (int j = 0; j < 150; j++) {
					threadPool--;
					threadName = "task" + j;
					service.execute(new PressureTestThreadWorker(redisTemplate, threadName, namePool[i]));
					if (threadPool <= 0) {
						threadPool = threadPoolDefault;
						service.shutdown();
						while (!service.isTerminated()) {
						}
					}
				}
				while (!service.isTerminated()) {
				}
				log.info(threadName + "============= 批次處理完畢");
				service = Executors.newFixedThreadPool(threadPoolDefault);
			}
			service.shutdown();
			while (!service.isTerminated()) {
			}
			time2 = System.currentTimeMillis();
			log.info(">>>>COST============花費:" + ((double) time2 - time1) / 1000 + "秒");
		} catch (Exception e) {
			e.printStackTrace();
			log.error(">>>>>>"+ e.getMessage());
		}
	}

	public static void main(String[] args) {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		ThreadTestRun ThreadTestRun = ctx.getBean(ThreadTestRun.class);
		ThreadTestRun.test();
	}

}
