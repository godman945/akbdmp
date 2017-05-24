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
		System.out.println("taskName:"+name);
		SetOperations<String, Object> opsForSet = redisTemplate.opsForSet();
		for (int i = 0; i < 1000000; i++) {
			boolean flag = true;
			int no = 0;
			while(flag){
				Random randData = new Random();
				no = randData.nextInt(20000000);
				flag = opsForSet.members(user).contains("code_"+no) ? true : false;
				opsForSet.add(user, "code_"+no);
				System.out.println(name+"***********"+i);
				flag = false;
			}
			System.out.println(user+">>>>> code_"+no);
//			System.out.println(name+" do "+i);
//			if(i == 2 && "task0".equals(name)){
//				try {
//					Thread.sleep(50000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
		}
	}


}
