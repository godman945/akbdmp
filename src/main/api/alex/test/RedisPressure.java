package alex.test;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.mongo.db.service.classcount.IClassCountService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.util.JedisClusterCRC16;

@Component
public class RedisPressure {

	Log log = LogFactory.getLog(RedisPressure.class);
	
	

	@Autowired
	RedisTemplate<String, String> redisTemplate;
	
	
	@Autowired
	DateFormatUtil dateFormatUtil;
	
	
	@Autowired
	private JedisConnectionFactory JedisConnectionFactory;
	
	
	@Autowired
	private IClassCountService classCountService;
	
	
	@SuppressWarnings("resource")
	private void redisTest() throws Exception{
		try{
			
			
			System.out.println(redisTemplate.opsForSet().difference("{TEST}_A01", "{TEST}_B01"));
			
			
			
			
//			Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//			jedisClusterNodes.add(new HostAndPort("192.168.2.204",6379));
//			jedisClusterNodes.add(new HostAndPort("192.168.2.205",6379));
//			jedisClusterNodes.add(new HostAndPort("192.168.2.206",6379));
//			jedisClusterNodes.add(new HostAndPort("192.168.2.207",6379));
//			jedisClusterNodes.add(new HostAndPort("192.168.2.208",6379));
//			jedisClusterNodes.add(new HostAndPort("192.168.2.209",6379));
//			JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes,0,1000);
//			
//			jedisCluster.sadd("alex".getBytes(), "1".getBytes());
//			JedisSlotBasedConnectionHandler connectionHandler = new JedisSlotBasedConnectionHandler(jedisClusterNodes, jedisConfig, 5000);
			//2604
//			System.out.println(JedisClusterCRC16.getSlot("{pchome}_alex"));
//			System.out.println(JedisClusterCRC16.getSlot("{pchome}_bessie"));
//			JedisConnectionFactory.getConnection().sAdd("{pchome}_bessie".getBytes(), "2".getBytes());
//			System.out.println(JedisConnectionFactory.getConnection().sDiff("{pchome}_alex".getBytes(),"{pchome}_bessie".getBytes()));
//			System.out.println(redisTemplate.opsForSet().difference("{pchome}_alex", "{pchome}_bessie"));
			
			
//			RedisClusterNode  redisClusterNode  = JedisConnectionFactory.getClusterConnection().clusterGetNodeForSlot(2604);
//			System.out.println(redisClusterNode.getHost());
//			redisClusterNode.servesSlot(2604);
//			
//			JedisPoolConfig jedisConfig = new JedisPoolConfig();
//			JedisSlotBasedConnectionHandler connectionHandler = new JedisSlotBasedConnectionHandler(jedisClusterNodes, jedisConfig, 5000);
//			connectionHandler.renewSlotCache();
//			connectionHandler.getConnection().sadd("bessie".getBytes(), "555".getBytes());
//			List<JedisPool> pools = new ArrayList<JedisPool>();
//			pools.addAll(cache.getNodes().values());
//			JedisClusterInfoCache clusterInfoCache = new RedisPressure();
			

			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
//			Jedis jedis = connectionHandler.getConnectionFromSlot(2604);
//			jedis.sadd("bessie", "5");
			
//			System.out.println(JedisCluster.HASHSLOTS);
			
			
			
//			 JedisPool pool = clusterInfoCache.getSlotPool(slot);
			
			
			
			
			
			
			
			
			
			
			
			
			
//			jedis.sadd("bessie", "2");
			
			
			
//			Jedis jedis = new Jedis("192.168.2.204:6379");
//			jedis.sadd("alex".getBytes(), "1".getBytes());
////			jedis.close();
//			System.out.println(jedis.scard("alex".getBytes()));
//			
//			Reshard.migrateSlots(srcNodeInfo, destNodeInfo, 9189);
//			
//			 List<Object> slots = jedisCluster.clusterSlots();  
//			System.out.println(jedisCluster.geohash("alex", "1"));
			
			
			
			
			
//			JedisClusterConnectionHandler connectionHandler = jedisCluster.co
			
			
			
//			System.out.println(jedisCluster.sdiffstore("alex", "bessie"));
			
//			System.out.println(jedisCluster.info());
			
//			log.info(jedisCluster.scard("test01"));
//			log.info(jedisCluster.scard("test02"));
//			System.out.println(jedisCluster.smembers("bessie"));
//			jedisCluster.srem("nico", "1234");
//			jedisCluster.sadd("nico", "N01");
			
//			jedisCluster.add
			
			
			
//			System.out.println(jedisCluster.smembers("nico"));
			
//			System.out.println(jedisCluster.sdiff("alex", "bessie"));
			
			
			
			
			
			
			
			
			
//			log.info(jedisCluster.sdiff("test01","test02"));
			
			
			
			
//			System.out.println(redisTemplate.opsForSet().size("test01"));
////			System.out.println(redisTemplate.opsForSet().difference("test01", "test02"));
////			redisTemplate.opsForSet().add("bessie", "123");
//			
//			
////			System.out.println(redisTemplate.opsForSet().members("alex"));
////			System.out.println(redisTemplate.opsForSet().members("bessie"));
//			System.out.println(redisTemplate.opsForSet().difference("alex", "bessie"));
			
			
			
			
//			RedisConnection con = JedisConnectionFactory.getConnection();
////			log.info(con.sCard("test01".getBytes()));
////			log.info(con.sCard("test02".getBytes()));
//			log.info(con.sDiff("alex".getBytes(),"bessie".getBytes()));
//			
//			Set<byte>
			
			
//			log.info(con.sMembers("test01".getBytes()));
//			log.info(con.sDiff("test01".getBytes(), "test02".getBytes()));
		}catch(Exception e){
			log.error(e.getMessage());
			System.exit(1);
		}
		
		
		
//		redisTemplate.opsForSet().add("TEST01", "CC09");
//		System.out.println(JedisConnectionFactory.getConnection().sCard("TEST01".getBytes()));
		
		
	
		
//		redisTemplate.delete("");
		
		
		
		
		
		
		
		
//		ClassCountMongoBean classCountMongoBean = new ClassCountMongoBean();
//		classCountMongoBean.setMemid("test");
//		classCountMongoBean.setUuid("test");
//		classCountMongoBean.setBehavior("campaign");
//		classCountMongoBean.setAd_class("test");
//		classCountMongoBean.setCount(10);
//		classCountMongoBean.setRecord_date("test");
//		classCountService.saveOrUpdate(classCountMongoBean);
		
		
//		Jedis jedis = new Jedis("redisdev.mypchome.com.tw");
//		
//		
//		jedis.set("LifeCheck", "I am fine");
//		
//		
//		
//		
//		System.out.println(jedis.get("LifeCheck"));
		
		
//		System.out.println(jedis.incrBy("alex".getBytes(), 10));
		
		
//		System.out.println(jedis.get("alex"));
		
//		jedis.close();
		
		
		
		
		
		
		
//		redisTemplate.opsForValue().set("alex", "5");
//		System.out.println(redisTemplate.opsForValue().get("alex"));
//		redisTemplate.opsForSet().add("alex", 1);
//		long count = redisTemplate.opsForValue().increment("alex2", 1);
//		System.out.println(count);
//		redisTemplate.expire("alex2", 30, TimeUnit.DAYS);
//		System.out.println(redisTemplate.opsForValue().get("alex2"));
		
		
//		long count = redisTemplate.opsForValue().increment("alex", 1);
		
////		System.out.println(count);
//		System.out.println(redisTemplate.opsForValue().get("alex"));
		
//		System.out.println(redisTemplate.opsForSet().members("alex"));
		
		
//		RedisClusterConnection connection = JedisConnectionFactory.getClusterConnection();
////		System.out.println(connection.exists("alex".getBytes()));
//		System.out.println(connection.sCard("A01".getBytes()));
		
//		redisTemplate.opsForSet().add("alex", "112");
//		redisTemplate.opsForSet().add("Nico", "code_1ec85663-5b75-46ce-a9bb-b25375b4c1f9");
		
		
//		System.out.println(connection.sMembers("alex".getBytes()).isEmpty());
//		System.out.println(connection.sInterStore("alex".getBytes(), "alex".getBytes()));
		
//		System.out.println(connection.sCard("alex".getBytes()));
//		connection.append("A".getBytes(), "A".getBytes());
		
		
//		code_65a5f83c-9b9a-4ad8-aa0c-7962842aad46
		
		
//		System.out.println(connection.cluster);
		
		
//		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
//		System.out.println(redisTemplate.opsForSet().members("A1"));
//		System.out.println(redisTemplate.opsForSet().members("B1"));
//		
//		System.out.println("nico,alex 差集: "+opsForSet.difference("A1", "B1")+" size:"+opsForSet.difference("nico", "alex").size());
		
		
//		System.out.println(redisTemplate.opsForSet().members("B1"));
//		redisTemplate.opsForSet().add("A1", "code_e22e3f4e-8fa6-4848-9479-6ea1dde1c0b0");
//		redisTemplate.opsForSet().add("A1", "a02");
//		redisTemplate.opsForSet().add("B1", "b01");
//		redisTemplate.opsForSet().add("B1", "a02");
//		System.out.println(connection.sInter("B1".getBytes(), "alex".getBytes()).size());
		
		
		
//		connection.sInter("B1".getBytes(), "alex".getBytes());
		
//		Set<byte[]> set = connection.sInter("B1".getBytes(), "alex".getBytes()).;
//		for (byte[] object : set) {
//			System.out.println(new String(object, "UTF-8"));
//		}
//		
//		
////		System.out.println(connection.sDiff("A1".getBytes(), "B1".getBytes()).size());
//		
//		
//		 System.out.println("nico:"+redisTemplate.opsForSet.members("nico") +" size:"+ redisTemplate.opsForSet.members("nico").size());
		
		
		
		
//		connection.del("alex".getBytes(),"nico".getBytes(),"Nico".getBytes(),"bessie".getBytes());
//		connection.close();
//		System.out.println(redisTemplate.opsForSet().members("nico"));
//		System.out.println(redisTemplate.opsForSet().difference("Nico", "bessie").size());
		
//		System.out.println(connection.bitCount("alex".getBytes()));
//		redisTemplate.opsForSet().
//		System.out.println(connection.clusterGetSlotForKey("alex".getBytes()));
//		Object obj = redisTemplate.getKeySerializer().deserialize("alex".getBytes());
//		System.out.println(obj);
//		System.out.println(redisTemplate.opsForSet().members(obj.toString()).contains("A"));
//		redisTemplate.delete("alex");
//		System.out.println(redisTemplate.opsForSet().members("Nico"));
//		ClusterOperations clusterOps = redisTemplate.opsForCluster();
//		redisTemplate.opsForCluster().
		
//		redisTemplate.delete("Nico");
		
		
		
//		ClusterOperations<String, Object>  f = redisTemplate.opsForCluster();
		
//		redisTemplate.opsForValue().set("nico", "444");
		
//		System.out.println(redisTemplate.opsForValue().get("nico"));
//		redisTemplate
		
//		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
////		System.out.println(opsForSet.members("test1").size());
////		System.out.println(opsForSet.members("alex").size());
////		System.out.println(redisTemplate.opsForValue().get("alex"));
//		
////		redisTemplate.opsForValue().set("TEST", "G");
//////		System.out.println(redisTemplate.opsForValue().get("TEST"));
////		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
//////		opsForSet.add("test2", "0001");
////		System.out.println(redisTemplate.opsForValue().get("TEST"));
////		redisTemplate.opsForValue().set("T101", "02");
////		redisTemplate.opsForValue().set("TEST2", "A");
//		
//		
////		redisTemplate.delete("TEST");
//		
//		
//		System.out.println(redisTemplate.opsForValue().get("544544"));
		
		
		
		
		
//		System.out.println(redisTemplate.hasKey("alex"));
//		opsForSet.add("test1", "alex_test01");
//		System.out.println(opsForSet.members("alex"));
		
//		System.out.println(opsForSet.members("test1").toString());
//		opsForSet.add("test1", "alex");
//		for (Object obj : opsForSet.members("test1")) {
//			System.out.println(obj);
//		}
//		
//		ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
//		opsForSet.add("AA", "ZZZZZZZZZZZzz");
		
//		log.info(opsForSet.members("nico").size());
//		Date date = new Date(); 
//		System.out.println("開始時間:"+dateFormatUtil.getDateTemplate2().format(date));
//		ForkJoinPool forkJoinPool = new ForkJoinPool();
//		Future<Integer> future = forkJoinPool.submit(new ForkJoinProcess(0,10000000,opsForSet)); 
//		System.out.println("最後完成的size=" + future.get());  
//		forkJoinPool.shutdown();
//		Date date2 = new Date(); 
//		System.out.println("結束時間:"+dateFormatUtil.getDateTemplate2().format(date2));
	}
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		RedisPressure redisPressure = (RedisPressure) ctx.getBean(RedisPressure.class);
		redisPressure.redisTest();
//		Random r = new Random();
//		int start = 1000000;
//		int end = 1015000;
//		int result = r.nextInt(end-start) + start;
//		System.out.println(result);
	}
}
