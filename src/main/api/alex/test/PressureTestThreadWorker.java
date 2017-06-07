package alex.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

public class PressureTestThreadWorker implements Runnable {
	Log log = LogFactory.getLog(this.getClass());

	private RedisTemplate<String, String> redisTemplate;
	private String taskName;
	private JedisConnectionFactory JedisConnectionFactory;
	public PressureTestThreadWorker(RedisTemplate<String, String> redisTemplate, String taskName,JedisConnectionFactory JedisConnectionFactory) {
		this.redisTemplate = redisTemplate;
		this.taskName = taskName;
		this.JedisConnectionFactory = JedisConnectionFactory;
	}

	public void run() {
		try{
			SetOperations<String, String> opsForSet = redisTemplate.opsForSet();
			log.info("redisTemplate:"+redisTemplate.opsForSet());
			String[] namePool = { "{PCHOME}_test01","{PCHOME}_test02","{PCHOME}_test03","{PCHOME}_test04","{PCHOME}_test05","{PCHOME}_test06","{PCHOME}_test07","{PCHOME}_test08","{PCHOME}_test09","{PCHOME}_test10" };
			long time1, time2;
			RedisClusterConnection connection = JedisConnectionFactory.getClusterConnection();
			
			time1 = System.currentTimeMillis();
			for (String userName : namePool) {
				for (int i = 0; i < 49500; i++) {
					time2 = System.currentTimeMillis();
					String guid = java.util.UUID.randomUUID().toString();
					opsForSet.add(userName, "code_" + guid);
					if(((double) time2 - time1) / 1000 >= 120){
						log.info(userName+">>>>>> size: "+connection.sCard(userName.getBytes()));
						time1 = time2;
					}
				}
				for (int j = 0; j < 500; j++) {
					time2 = System.currentTimeMillis();
					opsForSet.add(userName, taskName + "_" + j);
					if(((double) time2 - time1) / 1000 >= 120){
						log.info(userName+">>>>>> size: "+connection.sCard(userName.getBytes()));
						time1 = time2;
					}
				}
				log.info(taskName+"_"+userName+">>>>>> finish ================= ");
			}
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}
	
	
}
