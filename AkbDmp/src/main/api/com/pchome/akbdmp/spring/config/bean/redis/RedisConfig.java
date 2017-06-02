package com.pchome.akbdmp.spring.config.bean.redis;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
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
	
	@Value("${redis.server}")
	private String redisServer;

	 @Bean
	 JedisPoolConfig jedisPoolConfig() {
		 JedisPoolConfig jedisConfig = new JedisPoolConfig();
		 jedisConfig.setMaxIdle(maxIdle);
//		 jedisConfig.setMaxWaitMillis(maxWait);
		 jedisConfig.setTestOnBorrow(testOnBorrow);
		 jedisConfig.setMaxWaitMillis(-1);
		 
		//逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
		 jedisConfig.setTimeBetweenEvictionRunsMillis(-1);
		//在空闲时检查有效性, 默认false
		 jedisConfig.setTestWhileIdle(false);
		//设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
		 jedisConfig.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
		 return jedisConfig;
	 }

	//@Bean
	public RedisClusterConfiguration getClusterConfiguration() {
		Map<String, Object> source = new HashMap<String, Object>();
		source.put("spring.redis.cluster.nodes", redisServer);
		source.put("spring.redis.cluster.timeout", 5000);
		source.put("spring.redis.cluster.max-redirects", 8);
		return new RedisClusterConfiguration(new MapPropertySource("RedisClusterConfiguration", source));
	}

	//@Bean(name = "JedisConnectionFactory")
	public JedisConnectionFactory getConnectionFactory() {
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory(getClusterConfiguration());
		jedisConnectionFactory.setPoolConfig(jedisPoolConfig());
		return jedisConnectionFactory;
	}

	//@Bean
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
