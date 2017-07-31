package test.bessie;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.mongodb.Mongo;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassCountProdMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.PersonalInformationProdMongoBean;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategory;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryAudienceAnalyze;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.hadoopdmp.data.mysql.pojo.DmpTransferDataLog;
import com.pchome.hadoopdmp.mysql.db.service.category.IAdmCategoryAudienceAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.category.IAdmCategoryService;
import com.pchome.hadoopdmp.mysql.db.service.categorygroup.IAdmCategoryGroupService;
import com.pchome.hadoopdmp.mysql.db.service.transferdata.IDmpTransferDataLogService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.soft.util.DateFormatUtil;

@Component
public class TestRun {

//	Log log = LogFactory.getLog(TestRun.class);
	Log log = LogFactory.getLog("TestRun");

	@Autowired
	private DateFormatUtil dateFormatUtil;

	@Autowired
	private IAdmCategoryGroupService admCategoryGroupService;

	@Autowired
	private IAdmCategoryService admCategoryService;

	@Autowired
	private IAdmCategoryAudienceAnalyzeService admCategoryAudienceAnalyzeService;
	
	@Autowired
	private MongoOperations mongoOperations;
	
	@Autowired
	private IDmpTransferDataLogService dmpTransferDataLogService;
	

	
	private void age() throws UnknownHostException{
		
//		ClassCountMongoBean classCountMongoBean = null;
//		Query query = new Query(Criteria.where("user_id").is("863a0eb3-e02a-4372-93b5-0c020b2ff38f"));
//		classCountMongoBean = mongoOperations.findOne(query, ClassCountMongoBean.class);
		
		
//		Query queryNew = new Query(Criteria.where("user_id").is("863a0eb3-e02a-4372-93b5-0c020b2ff38f"));
//		List<ClassCountMongoBean> newAdLogUrlMongoBeanQuery = mongoOperations.find(queryNew, ClassCountMongoBean.class);
		
		
		List<ClassCountMongoBean> newAdLogUrlMongoBeanQuery = null;
		newAdLogUrlMongoBeanQuery = mongoOperations.findAll(ClassCountMongoBean.class,"class_count_test");
//		List<PcsProdMongoBean> pcsProdMongoBeanList = mongoOperations.findAll(PcsProdMongoBean.class, PcsMongoTableNameEnum.PCS_USER_PROD_TABLE_NAME.getValue());
		
		int ageInt = 0;
		int sum=0;
		for (ClassCountMongoBean classUrlMongoBean : newAdLogUrlMongoBeanQuery) {
			
//			System.out.println("user_id: "+classUrlMongoBean.getUser_id());
			
			
			String age =(String) classUrlMongoBean.getUser_info().get("age");
			
			 if (StringUtils.isNotBlank(age)){
				 ageInt = Integer.parseInt((String) classUrlMongoBean.getUser_info().get("age"));
				 if ((ageInt>100)){
//					 System.out.println("age: "+classUrlMongoBean.getUser_info().get("age"));
					 sum=sum+1;
					}
			 }
			
			
		}
//		System.out.println("size : "+newAdLogUrlMongoBeanQuery.size());
		
		System.out.println("sum : "+sum);
		
//		System.out.println("size : "+classCountMongoBean.getUser_id());
		
	}
	
	private void mongoRegex() throws UnknownHostException{
		MongoOperations newQueryMongoOperations = new MongoTemplate(new SimpleMongoDbFactory(new Mongo("mongodb.mypchome.com.tw", 27017), "dmp", new UserCredentials("webuser", "MonG0Dmp")));
//		List<ClassUrlMongoBean> newAdLogUrlMongoBeanQuery = null;
//		Query queryNew = new Query(Criteria.where("url").is("http://goods.ruten.com.tw/item/show?21508498919039"));
//		newAdLogUrlMongoBeanQuery = newQueryMongoOperations.find(queryNew, ClassUrlMongoBean.class);
		
		List<ClassUrlMongoBean> newAdLogUrlMongoBeanQuery = null;
		Query queryNew = new Query(Criteria.where("url").regex("[\"ruten\"]"));//.regex("ruten")
		queryNew.skip(0);
		queryNew.limit(10);
//		Query queryNew = new Query(Criteria.where("url").is("https://24h.pchome.com.tw/store/DIBMLA"));
		newAdLogUrlMongoBeanQuery = newQueryMongoOperations.find(queryNew, ClassUrlMongoBean.class);
		for (ClassUrlMongoBean classUrlMongoBean : newAdLogUrlMongoBeanQuery) {
			System.out.println("url : "+classUrlMongoBean.getUrl());
			System.out.println("ad_class : "+classUrlMongoBean.getAd_class());
		}
		
//		Query query = new Query();
//		query.addCriteria(Criteria.where("category_info.category").regex("0015022500000000").and("user_info.type").is("uuid"));
//		List<ClassCountMongoBean> classCountMongoList = mongoOperations.find(query, ClassCountMongoBean.class);
		
		
		
//		System.out.println("url : "+newAdLogUrlMongoBeanQuery.getUrl());
	}
	
	private void insertHql(){
		List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.loadAll();
		System.out.println("all size: "+list.size());
		
		AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
//		admCategoryAudienceAnalyze.setRecordDate(new Date());
		admCategoryAudienceAnalyze.setKeyId("test");
		admCategoryAudienceAnalyze.setKeyName("3C");
		admCategoryAudienceAnalyze.setKeyType("uuid");
		admCategoryAudienceAnalyze.setUserType("uuid");
		admCategoryAudienceAnalyze.setSource("24h");
		admCategoryAudienceAnalyze.setKeyCount(100);
		admCategoryAudienceAnalyze.setCreateDate(new Date());
		admCategoryAudienceAnalyze.setUpdateDate(new Date());
		admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);
		
		List<AdmCategoryAudienceAnalyze> loadAll=admCategoryAudienceAnalyzeService.loadAll();
		System.out.println("add size: "+loadAll.size());
	}
	
	private void deleteHql(){
		try{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		    Date convertedCurrentDate = sdf.parse("2017-07-06");
	
			String query = " from AdmCategoryAudienceAnalyze where recordDate = ? ";
		    Object[] queryParam = {convertedCurrentDate};//2017-07-05
		    List<AdmCategoryAudienceAnalyze> list= (List<AdmCategoryAudienceAnalyze>)admCategoryAudienceAnalyzeService.findHql(query, queryParam);
		    System.out.println("hql: "+list.size());
		    
		    admCategoryAudienceAnalyzeService.deleteAll(list);
		    
		    list=admCategoryAudienceAnalyzeService.loadAll();
		    System.out.println("delete all hql: "+list.size());
		    
		}catch(Exception e){
			e.getMessage();
		}
	}

	// //查詢
	private void hibernateDbTest() {
		List<AdmCategoryGroup> admAdGroupList = admCategoryGroupService.loadAll();
		System.out.println("Group size:" + admAdGroupList.size());
		for (AdmCategoryGroup admAdGroup : admAdGroupList) {
			System.out.println("Group name:" + admAdGroup.getGroupName());
			Set<AdmCategory> admAdClassSet = admAdGroup.getAdmCategories();
			for (AdmCategory admAdClass : admAdClassSet) {
				System.out.println(admAdClass.getAdClassName());
			}

		}
	}

	// 新增大類別
	private void hibernateDbTest2() throws Exception {
		Date date = new Date();
		String dateStr = dateFormatUtil.getDateTemplate2().format(date);
		date = dateFormatUtil.getDateTemplate2().parse(dateStr);

		AdmCategoryGroup admCategoryGroup = new AdmCategoryGroup();
		admCategoryGroup.setGroupName("運動類");
		admCategoryGroup.setGroupId("0000000000000003");
		admCategoryGroup.setCreateDate(date);
		admCategoryGroup.setUpdateDate(date);
		admCategoryGroupService.save(admCategoryGroup);

	}

	// 新增小類別
	private void hibernateDbTest3() throws Exception {
		AdmCategoryGroup admCategoryGroup = admCategoryGroupService.get(2);
		System.out.println(admCategoryGroup.getGroupId());

		Date date = new Date();
		String dateStr = dateFormatUtil.getDateTemplate2().format(date);
		date = dateFormatUtil.getDateTemplate2().parse(dateStr);

		AdmCategory admCategory = new AdmCategory();
		admCategory.setAdClass("AAAAAAAAAAAAAAAA");
		admCategory.setAdClassName("測試用");
		admCategory.setAdmCategoryGroup(admCategoryGroup);
		admCategory.setCreateDate(date);
		admCategory.setUpdateDate(date);
		admCategoryService.save(admCategory);
	}

	private void findByPage(){
		StringBuffer hql = new StringBuffer(" from AdmCategoryAudienceAnalyze where 1=1 ");
		hql.append(" and recordDate = '2017-07-07' ");
		hql.append(" and keyType  = '2' ");
		
		List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.findByPage(hql.toString(), 1, 200);
		System.out.println("all size: "+list.size());
		
		for (AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze : list) {
			System.out.println("id : " +admCategoryAudienceAnalyze.getId()+" , category : "+admCategoryAudienceAnalyze.getKeyId()+" , count:  "+admCategoryAudienceAnalyze.getKeyCount()+" , name : "+admCategoryAudienceAnalyze.getKeyName() +" , type : "+admCategoryAudienceAnalyze.getKeyType()+" , recordDate : "+admCategoryAudienceAnalyze.getRecordDate());
			
		}
		
	}
	
	private void hibernateGroupBy(){
		
		StringBuffer hql = new StringBuffer(" SELECT id, recordDate, keyId, keyName, keyType, userType, source, SUM( keyCount ) AS keyCount FROM  AdmCategoryAudienceAnalyze WHERE key_id =  '0000000000000001' GROUP BY recordDate, keyId, keyName, keyType, userType ");
//		hql.append(" and recordDate = '2017-07-07' ");
//		hql.append(" and keyType  = '2' ");
		List<AdmCategoryAudienceAnalyze> list =   admCategoryAudienceAnalyzeService.findByPage(hql.toString(), 1, 200);
		for (Object obj : list) {
			Object[] objArrAY = (Object[]) obj;
			System.out.println(objArrAY[7]);
			
		}
		
	}
	
	private void split() {
		try {
			// String str="0001004706430000_UUID";
			// String adclass=str.split("_")[0];
			// String type=str.split("_")[1];
			//
			// System.out.println(adclass);
			// System.out.println(type);

			// String str1="2017-06-20 16:04:40";
			// System.out.println("str1: "+str1.split(" ")[0]);

			// DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			// Date date = new Date();
			// String today=dateFormat.format(date);
			// System.out.println(today);

			// String age="01";
			// int ageInt=Integer.valueOf(age);
			// System.out.println("ageInt: "+ageInt);
			//
			// if ((ageInt>=1) && (ageInt<=10)){
			// System.out.println("age01to10");
			// }

			// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			// DateFormat df = DateFormat.getDateInstance();
			// Date date = df.parse("2017/07/01");
			// Calendar calendar = Calendar.getInstance();
			// calendar.setTime(date);
			// System.out.println(sdf.format(calendar.getTime()));

			// 欲轉換的日期字串
			// String dateString = "2017-07-01";
			// //設定日期格式
			// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			// //進行轉換
			// Date date = sdf.parse(dateString);
			// System.out.println(date);

//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//			Date convertedCurrentDate = sdf.parse("2013-09-18");
//			System.out.println(convertedCurrentDate);
			
			SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date current = new Date();
			String date = sdFormat.format(current);
		    Date today = sdFormat.parse(date);
		    System.out.println("date: "+date);
		    System.out.println("today: "+today);
	

			// String date=sdf.format(convertedCurrentDate );
			// System.out.println(date);

			// System.out.println(!StringUtils.equals("123", "124"));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// List指標
	private void listTest() {
		List<String> list = new ArrayList<>();
		list.add("a01");
		list.add("a02");
		list.add("a03");

		for (String string : list) {
			System.out.println(string);
			System.out.println(list.indexOf(string));
		}

	}

	private void newDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
		String dateString = sdf.format(new Date());
		System.out.println(dateString);
	}
	
	private void personalInfo() throws UnknownHostException{
		
		PersonalInformationProdMongoBean personalInformationProdMongoBean = new PersonalInformationProdMongoBean();
		Query userQuery = new Query(new Criteria().where("memid").is("zxc23th"));
		userQuery.with(new Sort(Sort.Direction.DESC, "_id"));
		personalInformationProdMongoBean = mongoOperations.findOne(userQuery, PersonalInformationProdMongoBean.class);
		System.out.println("age : "+personalInformationProdMongoBean.getAge());
		System.out.println("sex : "+personalInformationProdMongoBean.getSex());
		
	}
	
	public void ceil(){
		
		Query query = new Query(new Criteria().where("record_date").is("2016-08-01"));
		query.with(new Sort(Sort.Direction.ASC, "_id"));
		long  tatalcount =mongoOperations.count(query, ClassCountProdMongoBean.class);
		System.out.println("size : "+tatalcount);
		
		
		int pageIndex=0;
		int bulk=100;
		
		double pageSize= Math.ceil(((double)tatalcount)/bulk);
		
		
		while(pageIndex<pageSize){
			Query query1 = new Query(new Criteria().where("record_date").is("2016-08-01"));
			query1.with(new Sort(Sort.Direction.ASC, "_id"));
			query1.with(new PageRequest(pageIndex, bulk));
			List<ClassCountProdMongoBean> tatalcount1 =mongoOperations.find(query1, ClassCountProdMongoBean.class);
			
//			System.out.println("index : "+pageIndex+" -- "+" size : "+tatalcount1.size());
			pageIndex=pageIndex+1;
		}
		
//		System.out.println(pageSize);
//		System.out.println(Math.ceil(pageSize));
		
	}
	
	
	public void weightInfo(){
		//上一次權重
		double w = 0;
		double pExpv = Math.exp(-1 * 0.05);
		double age_w = w +(1 / (1 + pExpv));
		System.out.println("第1次權重  : "+age_w);
		
	}
	
	public void newton(){
		//新權重 = 上一次的權重 * Math.exp(-0.1 * (天*0.1));
		double w = 0.5124973964842103;
		double nw = w * Math.exp(- 0.1 * (20 * 0.1));
		
		System.out.println("牛頓冷卻  20 天 : "+nw);
		
		
		double pExpv = Math.exp(-1 * 0.05);
		double age_w = nw +(1 / (1 + pExpv));
		System.out.println("冷卻到負的權重  : "+age_w);
		
//		double w2 = 0.51249739648421;
//		double nw2 = w2 * Math.exp(- 0.1 * (2 * 0.1));
//		
//		System.out.println("newton day 2 : "+nw2);
//		
//		System.out.println("diff 1-2: "+(nw-nw2));
//		
//		double w3 = 0.51249739648421;
//		double nw3 = w3 * Math.exp(- 0.1 * (3 * 0.1));
//		
//		System.out.println("newton day 3 : "+nw3);
//		
//		System.out.println("diff 2-3: "+(nw2-nw3));
//		
//		double w4 = 0.51249739648421;
//		double nw4 = w4 * Math.exp(- 0.1 * (4 * 0.1));
//		
//		System.out.println("newton day 4 : "+nw4);
//		
//		System.out.println("diff 3-4: "+(nw3-nw4));
		
	}
	
	public void betwenDate() throws ParseException{
//		SimpleDateFormat simpleDateFormat = dateFormatUtil.getDateTemplate();
//		Date startDate = simpleDateFormat.parse("2017-07-20");
//		Date endDate = simpleDateFormat.parse("2017-07-20");
//		int betweenDate = (int) ((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
//		
//		System.out.println("betweenDate : "+betweenDate);
		
		double nw =-0.0001;
		if(nw<=0){//牛頓冷卻 到負值  ，直接給0
			nw=0;
			System.out.println("nw : "+nw);
		}else{
			System.out.println("nw : "+nw);
		}
			
	}	
	
	
	public void mongoQueryPage() throws ParseException{
		//開始時間
		long startTime = System.currentTimeMillis();
		System.out.println("startTime: " + startTime);
		
		Query query1 = new Query(new Criteria().where("record_date").is("2016-08-01"));
//		query1.with(new PageRequest(7, 1000));
		query1.with(new PageRequest(8, 10000,new Sort(Direction.ASC, "_id")));

		List<ClassCountProdMongoBean> classCountProdMongoBeanList = mongoOperations.find(query1,ClassCountProdMongoBean.class);

		//結束時間
		  long endTime = System.currentTimeMillis();
		  System.out.println("endTime: " + endTime);
		//執行時間
		  double totTime =(double)(endTime - startTime)/1000;
		//印出執行時間
		  System.out.println("Using Time:" + totTime+" sec");
		
	}
	
	
	
	
	public void mongoPageNoSort() throws ParseException{
		
		int pageIndex=0;
		
		while (pageIndex < 3) {

			Query query1 = new Query(new Criteria().where("record_date").is("2016-08-05"));
//			query1.with(new PageRequest(pageIndex, 5,new Sort(Direction.ASC, "_id")));
			query1.with(new PageRequest(pageIndex, 5));
			List<ClassCountProdMongoBean> classCountProdMongoBeanList = mongoOperations.find(query1,ClassCountProdMongoBean.class);
			
			for (ClassCountProdMongoBean classCountProdMongoBean : classCountProdMongoBeanList) {
				System.out.println("memid : "+classCountProdMongoBean.getMemid());
			}
			
			System.out.println("Page Index : "+pageIndex+"  "+" Page Size : "+classCountProdMongoBeanList.size());
			
			pageIndex=pageIndex+1;
		}
		
	}
	
	public void testJson() throws ParseException{
		
		Query query1 = new Query(new Criteria().where("user_id").is("dbae159e-e69b-4be9-8a3e-302bf2ad2401"));

		List<ClassCountMongoBean> classCountProdMongoBeanList = mongoOperations.find(query1,ClassCountMongoBean.class);
		
		for (ClassCountMongoBean classCountMongoBean : classCountProdMongoBeanList) {
			System.out.println(classCountMongoBean.getUser_info().get("memid"));
			
			if(classCountMongoBean.getUser_info().get("memid")==null){
				System.out.println("is null");
			}
		}
	}
	
	public void PersonalInfo() throws ParseException{
		String realPersonalInfo="";
		PersonalInformationProdMongoBean personalInformationProdMongoBean = null;
		
		if (StringUtils.isNotBlank("musashi99")) {
			Query userQuery = new Query(new Criteria().where("memid").is("musashi9900000"));
			userQuery.with(new Sort(Sort.Direction.DESC, "_id"));
			personalInformationProdMongoBean = mongoOperations.findOne(userQuery,
					PersonalInformationProdMongoBean.class);
			realPersonalInfo="1";
		} else if (StringUtils.isNotBlank("bb71e3b3-1079-48d2-adb4-874dc36e2c85")) {
			Query userQuery = new Query(new Criteria().where("uuid").is("bb71e3b3-1079-48d2-adb4-874dc36e2c85"));
			userQuery.with(new Sort(Sort.Direction.DESC, "_id"));
			personalInformationProdMongoBean = mongoOperations.findOne(userQuery,
					PersonalInformationProdMongoBean.class);
			realPersonalInfo="0";
		}
		
		System.out.println("memid : "+personalInformationProdMongoBean.getMemid());
		System.out.println("age : "+personalInformationProdMongoBean.getAge());
	}
	
	
	public void mongoNE() throws ParseException{
		
		Query query1 = new Query(new Criteria().where("memid").is("").and("uuid").ne("").and("age").ne("").and("sex").is(""));
		
//		 Criteria searchCriteria = Criteria.where("NAME").is("TestName").and("ID").ne("TestID").and("Age").is("23")
//				 "memid":"",
//				 "uuid":{"$ne":""},
//				 "age":{"$ne":""},
//				 "sex":{"$ne":""}
		
		query1.with(new PageRequest(0,10));
		
		List<ClassCountProdMongoBean> classCountProdMongoBeanList = mongoOperations.find(query1,ClassCountProdMongoBean.class);
		
		System.out.println("size :"+classCountProdMongoBeanList.size());
		
		for (ClassCountProdMongoBean classCountMongoBean : classCountProdMongoBeanList) {
			System.out.println(classCountMongoBean.getUuid());
		}
	}
	
	
	private void dmpTransferDataLog() {
		DmpTransferDataLog dmpTransferDataLog= new DmpTransferDataLog();
	    dmpTransferDataLog.setRecordDate("20170727");
	    dmpTransferDataLog.setStatus("success OK");
	    dmpTransferDataLogService.save(dmpTransferDataLog);
	}
	
	
	private void userDetail() {
//		Query query1 = new Query(new Criteria().where("memid").is("").and("uuid").ne("").and("age").ne("").and("sex").is(""));
		List<ClassCountMongoBean> classCountProdMongoBeanList = mongoOperations.findAll(ClassCountMongoBean.class);
		
		System.out.println("size :"+classCountProdMongoBeanList.size());
	}
	
	private void verifyTransferData(){
		
		Query queryCount = new Query();
		long tatalcount = mongoOperations.count(queryCount, ClassCountMongoBean.class);
		
		log.info("TestRun Total Size : " + tatalcount);
		
		int pageIndex = 0;
		int bulk = 10000;

		double pageSize = Math.ceil(((double) tatalcount) / bulk);
		
		log.info("TestRun pageSize : " + pageSize);
		
		Set<String> set = new HashSet<String>();
		
		String userId="";

		while (pageIndex < pageSize) {

			Query query1 = new Query();
			query1.with(new PageRequest(pageIndex, bulk));

			List<ClassCountMongoBean> classCountProdMongoBeanList = mongoOperations.find(query1,ClassCountMongoBean.class);

			log.info(">>>>>>>>>>>>> Page Index : " + pageIndex + " --  " + "Page Size : " + classCountProdMongoBeanList.size()+"    >>>>>>>>>>>>>");
		
			pageIndex = pageIndex + 1;
			
			for (ClassCountMongoBean classCountMongoBean : classCountProdMongoBeanList) {
				userId = classCountMongoBean.getUser_id();
				
				if (StringUtils.isNotBlank(userId)){
					set.add(userId);
				}
			}
		}
		
		log.info("TestRun set size : " + set.size());
	}
	
	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			TestRun TestRun = (TestRun) ctx.getBean(TestRun.class);
			// TestRun.hibernateDbTest();
			// TestRun.listTest();
			// TestRun.hibernateDbTest2();
			// TestRun.hibernateDbTest3();
			// TestRun.split();
//			 TestRun.deleteHql();
//			TestRun.split();
//			 TestRun.insertHql();
//			 TestRun.mongoRegex();
//			 TestRun.age();
//			TestRun.findByPage();
//			TestRun.hibernateGroupBy();
//			TestRun.newDate();
//			TestRun.personalInfo();
//			TestRun.ceil();
//			TestRun.weightInfo();
//			TestRun.newton();
//			TestRun.betwenDate();
//			TestRun.mongoQueryPage();
//			TestRun.mongoPageNoSort();
//			TestRun.testJson();
//			TestRun.PersonalInfo();
//			TestRun.mongoNE();
//			TestRun.dmpTransferDataLog();
//			TestRun.userDetail();
			TestRun.verifyTransferData();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

	}
}
