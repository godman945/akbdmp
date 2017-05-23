package alex.test;

import java.util.Random;
import java.util.concurrent.RecursiveTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.core.SetOperations;

public class ForkJoinProcess extends RecursiveTask<Integer> {

	private static final long serialVersionUID = 1L;
	
	Log log = LogFactory.getLog(ForkJoinProcess.class);
	
	//每個任務只處理10000筆資料
    private static final int MAX = 5000;
    private int start;  
    private int end;
    private int total;
    private SetOperations<String, Object> opsForSet;
    
    public ForkJoinProcess(int start, int end,SetOperations<String, Object> opsForSet) {  
        this.start = start;  
        this.end = end;
        this.opsForSet = opsForSet;
    }  
    
    protected Integer compute(){
        if ((end - start) < MAX) {  
        	log.info(">>>>>>:"+Thread.currentThread() + "process:" + start + " to " + end);
            for (int i = start; i < end; i++) {
            	total = total + 1;
            	try {
            		boolean flag = true;
            		int no = 0;
            		while(flag){
            			Random randData = new Random();
                		no = randData.nextInt(10000);
            			flag = opsForSet.members("alex").contains("code_"+no) ? true : false;
            			opsForSet.add("alex", "code_"+no);
            		}
            		System.out.println(">>>>> code_"+no);
				} catch (Exception e) {
					e.printStackTrace();
					 return -1;  
				}
            }  
//            return sum;  
            return total;
        } else {
            log.info(">>>>>> task disassemble");  
            //大任務分解兩個小任務
            int middle = (start + end) / 2;
            ForkJoinProcess left = new ForkJoinProcess(start,middle,opsForSet);  
            ForkJoinProcess right = new ForkJoinProcess(middle,end,opsForSet);  
            //執行兩個小任務  
            left.fork();  
            right.fork();  
            //小任務執行結果合併  
            return left.join() + right.join();  
        }  
    } 
}
