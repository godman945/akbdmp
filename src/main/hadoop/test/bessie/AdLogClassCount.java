package test.bessie;

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
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;
import com.pchome.soft.util.DateFormatUtil;

@Component
public class AdLogClassCount {
	Log log = LogFactory.getLog("TransferData");//MongoInsertClassUrl

//	@Autowired
//	private MongoOperations mongoOperations;// 正式機

	@Autowired
	private DateFormatUtil dateFormatUtil;

	@Autowired
	private WriteAkbDmp writeAkbDmp;

	public static MongoTemplate newDBMongoTemplate;// 測試機

	public void test() throws Exception {
		log.info("================START　PROCESS==========================");
		// 新的insert mongo 物件
		MongoOperations newDBMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(
				new Mongo("192.168.1.37", 27017), "pcbappdev", new UserCredentials("webuser", "axw2mP1i")));
		MongoTemplate newDBMongoTemplate = (MongoTemplate) newDBMongoOperations;
		newDBMongoTemplate.setWriteConcern(WriteConcern.SAFE);
		this.newDBMongoTemplate = newDBMongoTemplate;

		record();

		log.info("================END==========================");
	}

	public void record() throws Exception {// String date
		
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		MongoOperations oldMongoOperationsQuery = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
		
		// 先查詢總數
		Query queryCount = new Query(new Criteria().where("record_date").is("2016-08-01"));
		long tatalcount = oldMongoOperationsQuery.count(queryCount, ClassCountProdMongoBean.class);

		log.info("Total Size : " + tatalcount);

		int pageIndex = 0;
		int bulk = 100;

		double pageSize = Math.ceil(((double) tatalcount) / bulk);

		while (pageIndex < pageSize) {
			// .where("uuid").is("b2b8d3ba-edd1-4cdc-8e21-378c69eabf3b")
			Query query1 = new Query(new Criteria().where("record_date").is("2016-08-01"));
			query1.with(new PageRequest(pageIndex, bulk));

			List<ClassCountProdMongoBean> classCountProdMongoBeanList = oldMongoOperationsQuery.find(query1,ClassCountProdMongoBean.class);

			log.info(">>>>>>>>>>>>>Page Index : " + pageIndex + " --  " + "Page Size : " + classCountProdMongoBeanList.size()+"        ==============");

			pageIndex = pageIndex + 1;

			//讀取正式機符合日期的資料
			for (ClassCountProdMongoBean classCountProdMongoBean : classCountProdMongoBeanList) {
				
				String uuid = classCountProdMongoBean.getUuid();
				String memid = classCountProdMongoBean.getMemid();

				if (StringUtils.isBlank(uuid) && StringUtils.isBlank(memid)) {
					continue;
				}

				//先找會員個資1，再找uuid個資0
				String realPersonalInfo="";
				PersonalInformationProdMongoBean personalInformationProdMongoBean = null;
				if (StringUtils.isNotBlank(memid)) {
					Query userQuery = new Query(new Criteria().where("memid").is(memid));
					userQuery.with(new Sort(Sort.Direction.DESC, "_id"));
					personalInformationProdMongoBean = oldMongoOperationsQuery.findOne(userQuery,
							PersonalInformationProdMongoBean.class);
					realPersonalInfo="1";
				} else if (StringUtils.isNotBlank(uuid)) {
					Query userQuery = new Query(new Criteria().where("uuid").is(uuid));
					userQuery.with(new Sort(Sort.Direction.DESC, "_id"));
					personalInformationProdMongoBean = oldMongoOperationsQuery.findOne(userQuery,
							PersonalInformationProdMongoBean.class);
					realPersonalInfo="0";
				}
				
				//會員有資料
				if (StringUtils.isNotBlank(memid)){
					String user_id =memid;
					String ad_class = classCountProdMongoBean.getAd_class();
					String age = personalInformationProdMongoBean != null ? personalInformationProdMongoBean.getAge() : "";
					String sex = personalInformationProdMongoBean != null ? personalInformationProdMongoBean.getSex() : "";
					String source = classCountProdMongoBean.getBehavior();
					String type = "memid";
					String recodeDate = classCountProdMongoBean.getRecord_date();
	
					ClassCountLogBean classCountLogBean = new ClassCountLogBean();
					classCountLogBean.setAdClass(ad_class);
					classCountLogBean.setAge(age);
					classCountLogBean.setSex(sex);
					classCountLogBean.setUserId(user_id);
					classCountLogBean.setMemid(memid);
					classCountLogBean.setUuid("");
					classCountLogBean.setSource(source);
					classCountLogBean.setType(type);
					classCountLogBean.setRecordDate(recodeDate);
					classCountLogBean.setRealPersonalInfo(realPersonalInfo);
	
					log.info("memid_id : "+classCountProdMongoBean.get_id());
					saveUserInfo(classCountLogBean);
				}
				
				//uuid有資料
				if (StringUtils.isNotBlank(uuid)){
					String user_id =uuid;
					String ad_class = classCountProdMongoBean.getAd_class();
					String age = personalInformationProdMongoBean != null ? personalInformationProdMongoBean.getAge() : "";
					String sex = personalInformationProdMongoBean != null ? personalInformationProdMongoBean.getSex() : "";
					String source = classCountProdMongoBean.getBehavior();
					String type = "uuid";
					String recodeDate = classCountProdMongoBean.getRecord_date();
	
					ClassCountLogBean classCountLogBean = new ClassCountLogBean();
					classCountLogBean.setAdClass(ad_class);
					classCountLogBean.setAge(age);
					classCountLogBean.setSex(sex);
					classCountLogBean.setUserId(user_id);
					classCountLogBean.setMemid("");
					classCountLogBean.setUuid(uuid);
					classCountLogBean.setSource(source);
					classCountLogBean.setType(type);
					classCountLogBean.setRecordDate(recodeDate);
					classCountLogBean.setRealPersonalInfo(realPersonalInfo);
	
					log.info("uuid_id : "+classCountProdMongoBean.get_id());
					saveUserInfo(classCountLogBean);
				}
				
			}

		}
	}

	private void saveUserInfo(ClassCountLogBean classCountLogBean) throws Exception {
		writeAkbDmp.process(classCountLogBean);
	}

	public static void main(String[] args) {
		Log log = LogFactory.getLog("TransferData");//MongoInsertClassUrl
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			AdLogClassCount adLogUrlThread = ctx.getBean(AdLogClassCount.class);
			adLogUrlThread.test();
		} catch (Exception e) {
			log.info("Exception : "+e.getMessage());
			e.printStackTrace();
		}
	}
}
