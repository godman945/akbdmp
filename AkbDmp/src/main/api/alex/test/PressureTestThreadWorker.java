package alex.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

public class PressureTestThreadWorker implements Runnable {
	Log log = LogFactory.getLog(PressureTestThreadWorker.class);

	private RedisTemplate<String, Object> redisTemplate;
	private String name;

	public PressureTestThreadWorker(RedisTemplate<String, Object> redisTemplate, String taskName) {
		this.redisTemplate = redisTemplate;
		this.name = taskName;
	}

	@Override
	public void run() {
		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		String[] namePool = { "alex", "Nico", "bessie", "boris", "tim", "cool", "dyl", "park", "kylin", "hebe" };
		for (String userName : namePool) {
			for (int i = 0; i < 49500; i++) {
				String guid = java.util.UUID.randomUUID().toString();
				opsForSet.add(userName, "code_" + guid);
				log.info(userName + "_" + name + " do " + i);
			}
			for (int j = 0; j < 500; j++) {
				opsForSet.add(userName, name + "_" + j);
				log.info(userName + "_" + name + " do" + j + " >>>>>> " + name + "_" + j);
			}
			
		}
	}
}
