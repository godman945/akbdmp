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
//			SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
			SetOperations<String, Object> opsForSet = getRedisTemplate().opsForSet();
			log.info("redisTemplate:"+redisTemplate.opsForSet());
//			String[] namePool = { "alex", "Nico", "bessie", "boris", "tim", "cool", "dyl", "park", "kylin", "hebe" };
			String[] namePool = { "alex"};
//			long time1, time2;
			RedisClusterConnection connection = JedisConnectionFactory.getClusterConnection();
			
//			time1 = System.currentTimeMillis();
			for (String userName : namePool) {
				for (int i = 0; i < 49500; i++) {
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
			log.info(e.getMessage());
		}

	}
	
	
	
	
	
	
	
	
	
	
	 JedisPoolConfig jedisPoolConfig() {
		 JedisPoolConfig jedisConfig = new JedisPoolConfig();
		 jedisConfig.setMaxIdle(200);
		 jedisConfig.setMaxWaitMillis(300000);
//		 jedisConfig.setTestOnBorrow(testOnBorrow);
		 jedisConfig.setMaxWaitMillis(-1);
		 
		//逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
		 jedisConfig.setTimeBetweenEvictionRunsMillis(-1);
		//在空闲时检查有效性, 默认false
		 jedisConfig.setTestWhileIdle(false);
		//设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
		 jedisConfig.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
		 return jedisConfig;
	 }

	public RedisClusterConfiguration getClusterConfiguration() {
		Map<String, Object> source = new HashMap<String, Object>();
		source.put("spring.redis.cluster.nodes", "192.168.2.207:6379,192.168.2.204:6379,192.168.2.205:6379,192.168.2.208:6379,192.168.2.209:6379,192.168.2.206:6379");
		source.put("spring.redis.cluster.timeout", 5000);
		source.put("spring.redis.cluster.max-redirects", 8);
		return new RedisClusterConfiguration(new MapPropertySource("RedisClusterConfiguration", source));
	}

	public JedisConnectionFactory getConnectionFactory() {
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(getClusterConfiguration());
		jedisConnectionFactory.setPoolConfig(jedisPoolConfig());
		return jedisConnectionFactory;
	}

	public JedisClusterConnection getJedisClusterConnection() {
		return (JedisClusterConnection) getConnectionFactory().getConnection();
	}

	public RedisTemplate<String, Object> getRedisTemplate() {
		RedisTemplate<String, Object> clusterTemplate = new RedisTemplate<String, Object>();
		clusterTemplate.setConnectionFactory(getConnectionFactory());
		clusterTemplate.setKeySerializer(new StringRedisSerializer());
		clusterTemplate.setDefaultSerializer(new GenericJackson2JsonRedisSerializer());
		return clusterTemplate;
	}
	
	
	
	
	
	
}
