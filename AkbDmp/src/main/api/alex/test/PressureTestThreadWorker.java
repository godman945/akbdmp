package alex.test;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

public class PressureTestThreadWorker implements Runnable {

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
		for (int i = 0; i < 905000; i++) {
			String guid = java.util.UUID.randomUUID().toString();
			opsForSet.add(user, "code_"+guid);
			System.out.println(user+" do"+i+" >>>>>> code_"+guid);
		}
		for (int i = 0; i < 5000; i++) {
			opsForSet.add(user, name+"_"+i);
			System.out.println(user+" do"+i+" >>>>>> "+name+"_"+i);
		}
	}
}
