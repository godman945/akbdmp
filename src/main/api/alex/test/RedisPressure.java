package alex.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.mongo.db.service.classcount.IClassCountService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

@Component
public class RedisPressure {

	Log log = LogFactory.getLog(RedisPressure.class);
	
	

	@Autowired
	RedisTemplate<String, Object> redisTemplate;
	
	
	@Autowired
	DateFormatUtil dateFormatUtil;
	
	
	@Autowired
	private JedisConnectionFactory JedisConnectionFactory;
	
	
	@Autowired
	private IClassCountService classCountService;
	
	private void redisTest() throws Exception{
		try{
			RedisConnection con = JedisConnectionFactory.getConnection();
			log.info(con.sCard("test01".getBytes()));
			log.info(con.sCard("test02".getBytes()));
			log.info(con.sMembers("test01".getBytes()));
//			log.info(con.sDiff("test01".getBytes(), "test02".getBytes()));
		}catch(Exception e){
			log.error(e.getMessage());
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
