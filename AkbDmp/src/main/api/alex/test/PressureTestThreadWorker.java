package alex.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.JedisPoolConfig;

public class PressureTestThreadWorker implements Runnable {
	Log log = LogFactory.getLog(PressureTestThreadWorker.class);

	private RedisTemplate<String, Object> redisTemplate;
	private String taskName;
	private JedisConnectionFactory JedisConnectionFactory;
	public PressureTestThreadWorker(RedisTemplate<String, Object> redisTemplate, String taskName,JedisConnectionFactory JedisConnectionFactory) {
		this.redisTemplate = redisTemplate;
		this.taskName = taskName;
		this.JedisConnectionFactory = JedisConnectionFactory;
	}

	@Override
	public void run() {
		try{
			SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
			log.info("redisTemplate:"+redisTemplate.opsForSet());
			String[] namePool = { "alex", "Nico", "bessie", "boris", "tim" };
//			long time1, time2;
			RedisClusterConnection connection = JedisConnectionFactory.getClusterConnection();
			
//			time1 = System.currentTimeMillis();
			for (String userName : namePool) {
				for (int i = 0; i < 4500; i++) {
//					time2 = System.currentTimeMillis();
					String guid = java.util.UUID.randomUUID().toString();
					opsForSet.add(userName, "code_" + guid);
//					log.info(userName+">>>>>> process: "+i);
//					if(((double) time2 - time1) / 1000 >= 1800){
//						log.info(userName+">>>>>> size: "+connection.sCard(userName.getBytes()));
//						time1 = time2;
//					}
				}
				for (int j = 0; j < 500; j++) {
//					time2 = System.currentTimeMillis();
					opsForSet.add(userName, taskName + "_" + j);
//					log.info(userName+">>>>>> taskName: "+j);
//					if(((double) time2 - time1) / 1000 >= 1800){
//						log.info(userName+">>>>>> size: "+connection.sCard(userName.getBytes()));
//						time1 = time2;
//					}
				}
				log.info(taskName+"_"+userName+">>>>>> finish ================= ");
			}
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}
	
	
}
