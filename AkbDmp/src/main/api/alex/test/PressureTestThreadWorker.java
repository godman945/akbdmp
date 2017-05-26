package alex.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

public class PressureTestThreadWorker implements Runnable {
	Log log = LogFactory.getLog(PressureTestThreadWorker.class);	
	
	private RedisTemplate<String, Object> redisTemplate;
	private String name;
	private String user;
	public PressureTestThreadWorker(RedisTemplate<String, Object> redisTemplate,String name,String user) {
		this.redisTemplate = redisTemplate;
		this.name = name;
		this.user = user;
	}

	@Override
	public void run() {
		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		for (int i = 0; i < 62000; i++) {
			String guid = java.util.UUID.randomUUID().toString();
			opsForSet.add(user, "code_"+guid);
			log.info(name+"_"+user+" do"+i+" >>>>>> code_"+guid);
		}
		for (int i = 0; i < 500; i++) {
			opsForSet.add(user, name+"_"+i);
			log.info(name+"_"+user+" do"+i+" >>>>>> "+name+"_"+i);
		}
	}
}
