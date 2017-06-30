package test.bessie;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.pchome.akbdmp.job.bean.ClassCountLogBean;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassCountProdMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.PersonalInformationProdMongoBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.soft.util.DateFormatUtil;

@Component
public class AdLogClassCount {
	Log log = LogFactory.getLog("MongoInsertClassUrl");

	@Autowired
	private MongoOperations mongoOperations;
	
	@Autowired
	private DateFormatUtil dateFormatUtil;
	
	@Autowired
	private WriteAkbDmp writeAkbDmp;
	
	public static MongoTemplate newDBMongoTemplate;
	
	public void test() throws Exception {
		log.info("================START　PROCESS==========================");
		//新的insert mongo 物件
		MongoOperations newDBMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo("192.168.1.37", 27017), "pcbappdev", new UserCredentials("webuser", "axw2mP1i")));
		MongoTemplate newDBMongoTemplate = (MongoTemplate)newDBMongoOperations;
		newDBMongoTemplate.setWriteConcern(WriteConcern.SAFE);
		this.newDBMongoTemplate = newDBMongoTemplate;
		
		
		int start = 0;
		boolean processFlag = false;
		processFlag = record(start);
		while(processFlag){
			log.info(">>>>>>page:"+start);
			start = start + 1;
			processFlag = record(start);
		}
		log.info("================START　END==========================");
	}

	
	public boolean processDate(String dateStr) throws Exception{
		Date recodeDate = dateFormatUtil.getDateTemplate().parse(dateStr);
		Calendar recodeDateCalender = Calendar.getInstance();
		recodeDateCalender.setTime(recodeDate);
		
		Date now = new Date();
		Calendar nowCalendar = Calendar.getInstance();
		nowCalendar.setTime(now);
		
		long timeLong = nowCalendar.getTimeInMillis() - recodeDateCalender.getTimeInMillis();
		timeLong = timeLong/(24*60*60*1000);
		long index = 3600;
		if(timeLong > index){
//			log.info(">>>>>>>>>"+dateStr+": 離現在"+timeLong+"天前");
		}
		return timeLong > index ? false : true;
	}

	public boolean record(int start) throws Exception {
		String date = "";
		boolean flag = false;
//		.where("uuid").is("729faa4c-1164-43f4-b891-c5b1be0818ed")
		Query query = new Query(new Criteria());
		query.with(new Sort(Sort.Direction.DESC, "_id"));
		query.with(new PageRequest(start, 1000));
		
		List<ClassCountProdMongoBean> classCountProdMongoBeanList = mongoOperations.find(query, ClassCountProdMongoBean.class);
		for (ClassCountProdMongoBean classCountProdMongoBean : classCountProdMongoBeanList) {
			date = classCountProdMongoBean.getRecord_date();
			flag = processDate(date);
			if(!flag){
				log.info(" STOP　CREATE >>>>>>"+classCountProdMongoBean.get_id());
				break;
			}else{
				
				String uuid = classCountProdMongoBean.getUuid();
				String memid = classCountProdMongoBean.getMemid();
				
				if(StringUtils.isBlank(uuid) || StringUtils.isBlank(memid)){
					continue;
				}
				
				PersonalInformationProdMongoBean personalInformationProdMongoBean = null;
				if(StringUtils.isNotBlank(memid)){
					Query userQuery = new Query(new Criteria().where("memid").is(memid));
					personalInformationProdMongoBean = mongoOperations.findOne(userQuery, PersonalInformationProdMongoBean.class);
				}else if(StringUtils.isNotBlank(uuid)){
					Query userQuery = new Query(new Criteria().where("uuid").is(uuid));
					personalInformationProdMongoBean = mongoOperations.findOne(userQuery, PersonalInformationProdMongoBean.class);
				}
				
				
				String user_id = StringUtils.isNotBlank(memid) ? memid : uuid;
				String ad_class = classCountProdMongoBean.getAd_class();
				String age = personalInformationProdMongoBean != null ? personalInformationProdMongoBean.getAge() : "";
				String sex = personalInformationProdMongoBean != null ? personalInformationProdMongoBean.getSex() : "";
				String source = classCountProdMongoBean.getBehavior();
				String type = StringUtils.isNotBlank(classCountProdMongoBean.getMemid()) ? "memid" : "uuid";
				String recodeDate = classCountProdMongoBean.getRecord_date();
					
				ClassCountLogBean classCountLogBean = new ClassCountLogBean();
				classCountLogBean.setAdClass(ad_class);
				classCountLogBean.setAge(age);
				classCountLogBean.setSex(sex);
				classCountLogBean.setUserId(user_id);
				classCountLogBean.setMemid(memid);
				classCountLogBean.setUuid(uuid);
				classCountLogBean.setSource(source);
				classCountLogBean.setType(type);
				classCountLogBean.setRecordDate(recodeDate);
				
				
				System.out.println(user_id);
				saveUserInfo(classCountLogBean);
			}
		}
		return flag;
	}
	
	
	
	private void saveUserInfo(ClassCountLogBean classCountLogBean) throws Exception{
		writeAkbDmp.process(classCountLogBean);
		if(StringUtils.isNotBlank(classCountLogBean.getMemid()) && StringUtils.isNotBlank(classCountLogBean.getUuid())){
			classCountLogBean.setUserId(classCountLogBean.getUuid());
			classCountLogBean.setType("uuid");
			writeAkbDmp.process(classCountLogBean);
		}
	}
	
	
	
	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "prd");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			AdLogClassCount adLogUrlThread = ctx.getBean(AdLogClassCount.class);
			adLogUrlThread.test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
