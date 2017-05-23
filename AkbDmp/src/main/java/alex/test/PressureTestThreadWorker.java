package alex.test;

import java.util.Random;

import org.springframework.data.redis.core.SetOperations;

public class PressureTestThreadWorker implements Runnable {

	private SetOperations<String, Object> opsForSet;
	private int size;
	public PressureTestThreadWorker(SetOperations<String, Object> opsForSet,int size) {
		this.opsForSet = opsForSet;
		this.size = size;
	}

	@Override
	public void run() {
		addRedis();

	}

	private void addRedis() {
		try {
			boolean flag = true;
			int no = 0;
			while (flag) {
				Random randData = new Random();
				no = randData.nextInt(3000000);
				flag = opsForSet.members("alex").contains("code_" + no) ? true : false;
				opsForSet.add("alex", "code_" + no);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
