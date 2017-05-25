package alex.test;

import java.util.Random;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

public class PressureTestThreadWorker implements Runnable {

	private RedisTemplate redisTemplate;
	private String name;
	private String user;
	public PressureTestThreadWorker(RedisTemplate redisTemplate,String name,String user) {
		this.redisTemplate = redisTemplate;
		this.name = name;
		this.user = user;
	}

	@Override
	public void run() {
//		System.out.println(Thread.currentThread().getName());
//		System.out.println("taskName:"+name);
		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		for (int i = 0; i < 15000; i++) {
			String guid = java.util.UUID.randomUUID().toString();
			opsForSet.add(user, "code_"+guid);
			
			System.out.println(name+">>>>>>do "+i);
			System.out.println(user+">>>>>> code_"+guid);
		}
		for (int i = 0; i < 5000; i++) {
			boolean flag = true;
    		int no = 0;
    		while(flag){
    			Random randData = new Random();
        		no = randData.nextInt(5000);
    			flag = opsForSet.members(user).contains("test_"+no) ? true : false;
    			opsForSet.add(user, "test_"+no);
    			System.out.println(name+">>>>>>do "+i);
    			System.out.println(user+">>>>>> "+"test_"+no +" flag:"+flag);
    		}
		}
	}
}
