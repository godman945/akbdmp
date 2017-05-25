package com.pchome.akbdmp.spring.config.bean.redis;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfig {

	@Value("${redis.server}")
	private String redisHost;

	@Value("${redis.port}")
	private int redisPort;

	@Value("${redis.pool.maxIdle}")
	private int maxIdle;

	@Value("${redis.pool.maxWait}")
	private int maxWait;

	@Value("${redis.pool.testOnBorrow}")
	private boolean testOnBorrow;

	 @Bean
	 JedisPoolConfig jedisPoolConfig() {
		 JedisPoolConfig jedisConfig = new JedisPoolConfig();
		 jedisConfig.setMaxIdle(maxIdle);
		 jedisConfig.setMaxWaitMillis(maxWait);
		 jedisConfig.setTestOnBorrow(testOnBorrow);
		 return jedisConfig;
	 }
	//
	// @Bean
	// JedisConnectionFactory jedisConnectionFactory() {
	// JedisConnectionFactory factory = new JedisConnectionFactory();
	// factory.setHostName(redisHost);
	// factory.setPort(redisPort);
	// factory.setPoolConfig(jedisPoolConfig());
	// return factory;
	// }
	//
	// @Bean(name = "redisTemplate")
	// RedisTemplate<String, Object> redisTemplate() {
	// final RedisTemplate<String, Object> template = new RedisTemplate<String,
	// Object>();
	// template.setConnectionFactory(jedisConnectionFactory());
	// template.setKeySerializer(new StringRedisSerializer());
	// template.setHashValueSerializer(new
	// GenericToStringSerializer<Object>(Object.class));
	// template.setValueSerializer(new
	// GenericToStringSerializer<Object>(Object.class));
	// return template;
	// }

	@Bean
	public RedisClusterConfiguration getClusterConfiguration() {
		Map<String, Object> source = new HashMap<String, Object>();
		source.put("spring.redis.cluster.nodes", "192.168.2.207:6379,192.168.2.204:6379,192.168.2.205:6379,192.168.2.208:6379,192.168.2.209:6379,192.168.2.206:6379");
		source.put("spring.redis.cluster.timeout", 2000);
		source.put("spring.redis.cluster.max-redirects", 8);
		return new RedisClusterConfiguration(new MapPropertySource("RedisClusterConfiguration", source));
	}

	@Bean
	public JedisConnectionFactory getConnectionFactory() {
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(getClusterConfiguration());
		jedisConnectionFactory.setPoolConfig(jedisPoolConfig());
		return jedisConnectionFactory;
	}

	@Bean
	public JedisClusterConnection getJedisClusterConnection() {
		return (JedisClusterConnection) getConnectionFactory().getConnection();
	}

	@Bean(name = "redisTemplate")
	public RedisTemplate<String, Object> getRedisTemplate() {
		RedisTemplate<String, Object> clusterTemplate = new RedisTemplate<String, Object>();
		clusterTemplate.setConnectionFactory(getConnectionFactory());
		clusterTemplate.setKeySerializer(new StringRedisSerializer());
		clusterTemplate.setDefaultSerializer(new GenericJackson2JsonRedisSerializer());
		return clusterTemplate;
	}

}
