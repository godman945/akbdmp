package alex.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;


public class MongoThreadProcess implements Callable<Integer> {

	private MongoOperations mongoOperations; 
	private int start;
	private int end;
	private DBCursor cursor;
	private DBCollection dBCollection;
	Log log = LogFactory.getLog(MongoThreadProcess.class);
	
	public MongoThreadProcess(int start,int end,DBCursor cursor,DBCollection dBCollection) {
		this.mongoOperations = mongoOperations;
		this.start = start;
		this.end = end;
		this.cursor = cursor;
		this.dBCollection = dBCollection;
	}
	
	public synchronized Integer call() throws Exception {
		System.out.println("********** "+Thread.currentThread().getName()+"--->start" + " start:"+start + " end:"+end);
		int count = 0;
		int index = 0;
		while(this.cursor.hasNext()) {
			if(index >= start && index <= end){
				DBObject a = cursor.next();
				if(this.dBCollection.findOne(a) != null){
					System.out.println(Thread.currentThread().getName()+ " delete " +a.get("_id"));
					this.dBCollection.remove(a);
					count = count + 1;
				}
			}
			index ++;
		}
		
		
//		System.out.println("********** "+Thread.currentThread().getName()+"--->startPage:"+startPage);
//		System.out.println("********** "+Thread.currentThread().getName()+"--->endPage:"+endPage);
		
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		int page = startPage;
//		int size = 10000;
//		int range = 365;
//		int count = 0;
//		long time1, time2;
//		
//		boolean flag = true;
//		while(flag){
//			time1 = System.currentTimeMillis();
//			Pageable pageableRequest = new PageRequest(page, size);
//			Query query = new Query();
//			query.addCriteria(Criteria.where("user_info.type").is("uuid"));
//			query.with(pageableRequest);
//			query.with(new Sort(Direction.DESC, "_id"));
//			List<UserDetailMongoBean> listUser = mongoOperations.find(query,UserDetailMongoBean.class);
//			int processSize = listUser.size();
//			
//			
////			System.out.println("********** "+Thread.currentThread().getName()+"--->page:"+page);
////			System.out.println("********** "+Thread.currentThread().getName()+"--->size:"+processSize);
//			if(processSize == 0){
//				flag = false;
//			}
//			
//			for (UserDetailMongoBean userDetailMongoBean : listUser) {
//				Date date1 = sdf.parse(userDetailMongoBean.getCreate_date());
//				Date date2 = sdf.parse(userDetailMongoBean.getUpdate_date());
//				
//				Calendar cal1 = Calendar.getInstance();
//		        cal1.setTime(date1);
//		        
//		        Calendar cal2 = Calendar.getInstance();
//		        cal2.setTime(date2);
//		        int rangeDay = ( int ) ((date2.getTime() - date1.getTime()) / (1000*3600*24 )); 
//				if(rangeDay > range){
//					count = count + 1;
////					System.out.println("********** need delete start **********");
////					System.out.println("*************差異天數:" +rangeDay);
////					System.out.println(userDetailMongoBean.get_id());
////					mongoOperations.remove(Query.query(Criteria.where("_id").is(userDetailMongoBean.get_id())), UserDetailMongoBean.class);
////					System.out.println("********** need delete end **********");
//				}
//			}
//			log.info("********** "+Thread.currentThread().getName()+" ********************* process page:" + page);
//			page = page + 1;
//			
//			log.info("**********" +Thread.currentThread().getName()+":need delete count:"+count);
//			time2 = System.currentTimeMillis();
//			log.info(Thread.currentThread().getName()+" >>>>>>>> cost " + (time2-time1)/1000 + " sec");
//			if(page > endPage){
//				flag = false;
//			}
//		}
		return count;
	}
}
