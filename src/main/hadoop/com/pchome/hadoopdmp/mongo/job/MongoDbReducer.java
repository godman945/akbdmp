package com.pchome.hadoopdmp.mongo.job;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.IKdclStatisticsSourceService;
import com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource.KdclStatisticsSourceService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class MongoDbReducer extends Reducer<Text, Text, Text, Text> {

	Log log = LogFactory.getLog(this.getClass());

	SimpleDateFormat sdf = null;
	private final static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	
	List<JSONObject> kafkaList = new ArrayList<>();

	Producer<String, String> producer = null;
	long count = 0;
	String sizeCount;
	public void setup(Context context) {
		log.info(">>>>>> mongoDb Reduce  setup>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {

		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) {
		log.info(">>>>>> mongoDb Reduce reduce >>>>>>>>>>>>>>>>>>>>>>>>>>");
		log.info(">>>>>> mongoDb Reduce key >>>>>>>>>>>>>>>>>>>>>>>>>>"+key);
		log.info(">>>>>> mongoDb Reduce value >>>>>>>>>>>>>>>>>>>>>>>>>>"+value);
		try {
			count = count + 1;
			context.write(new Text(key), new Text("1"));
			
			if(key.equals("sizeCount")){
				sizeCount = value.toString();
			}
			
			
		} catch (Exception e) {
			log.info("reduce error"+e.getMessage());
			log.error(key, e);
		}

	}

	public void cleanup(Context context) {
		log.info(">>>>>> mongoDb Reduce  cleanup >>>>>>>>>>>>>>>>>>>>>>>>>>");
		log.info(">>>>>> count:"+count);
	}

	
	
	public void savekdclStatisticsSource(String idType,String serviceType,String behavior,String classify ,int count,String recodeDate,Date date,IKdclStatisticsSourceService kdclStatisticsSourceService) throws Exception{
//		KdclStatisticsSource kdclStatisticsSource = new KdclStatisticsSource();
//		kdclStatisticsSource.setIdType(idType);
//		kdclStatisticsSource.setServiceType(serviceType);
//		kdclStatisticsSource.setClassify(classify);
//		kdclStatisticsSource.setBehavior(behavior);
//		kdclStatisticsSource.setCounter(count);
//		kdclStatisticsSource.setRecordDate(recodeDate);
//		kdclStatisticsSource.setUpdateDate(date);
//		kdclStatisticsSource.setCreateDate(date);
//		kdclStatisticsSourceService.save(kdclStatisticsSource);
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		IKdclStatisticsSourceService kdclStatisticsSourceService = (KdclStatisticsSourceService) ctx.getBean(KdclStatisticsSourceService.class);
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		
//		int a = 0;
//		Calendar calendar = Calendar.getInstance();  
//		if(calendar.get(Calendar.HOUR_OF_DAY) == 16){
//			calendar.add(Calendar.DAY_OF_MONTH, -1); 
//			System.out.println(sdf.format(calendar.getTime()));;
//		}
//		KdclStatisticsSource kdclStatisticsSource = kdclStatisticsSourceService.findKdclStatisticsSourceByBehaviorAndRecordDate("7", sdf.format(calendar.getTime()));
//		a = a + kdclStatisticsSource.getCounter();
//		
//		System.out.println(a);
//		
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("7", sdf.format(calendar.getTime()));
//		
//		
//		a = a + 10;
//		
		
		
//		String recodeDate = sdf.format(date);
//		System.out.println(recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ad_click", recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("24h", recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("ruten", recodeDate);
//		kdclStatisticsSourceService.deleteByBehaviorAndRecordDate("personal_info", recodeDate);
		
		
//		UserDetailService userDetailService = (UserDetailService) ctx.getBean(UserDetailService.class);
//		UserDetailMongoBean userDetailMongoBean = userDetailService.findUserId("zxc910615");
//		
//		System.out.println(userDetailMongoBean.get_id());
//		System.out.println((String)userDetailMongoBean.getUser_info().get("sex"));
		
		
		
		
		
		
		
//		Query query = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
//		UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(query, UserDetailMongoBean.class);
//		userDetailMongoBean.getUser_info().get("sex")
//		userDetailMongoBean.getUser_info().get("age")
		
		
		
		
//		System.out.println(userDetailMongoBean.getCategory_info().get(0).get("category"));
		
		
//		KdclStatisticsSource kdclStatisticsSource = new KdclStatisticsSource();
//		kdclStatisticsSource.setClassify("A");
//		kdclStatisticsSource.setIdType("alex");
//		kdclStatisticsSource.setServiceType("9");
//		kdclStatisticsSource.setBehavior("GGG");
//		kdclStatisticsSource.setCounter(0);
//		kdclStatisticsSource.setRecordDate("2018-01-25");
//		kdclStatisticsSource.setUpdateDate(new Date());
//		kdclStatisticsSource.setCreateDate(new Date());
//		kdclStatisticsSourceService.save(kdclStatisticsSource);
	}


}
