package com.pchome.akbdmp.job.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pchome.akbdmp.job.bean.ClassCountLogBean;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.soft.depot.utils.KafkaUtil;

@Component
@Scope("prototype")
public class CampaignJob {

	Log log = LogFactory.getLog(this.getClass());

	@Value("${save.path.campaignlog}")
	private String campaignlogPath;

	@Autowired
	private MongoOperations mongoOperations;
	

	@Autowired
	private ObjectMapper objectMapper;
	
	@Autowired
	private KafkaUtil kafkaUtil;
	
	private final static String BEHAVIOR = "campaign";

	public void process2() throws Exception {
		
		Set<String> set = new HashSet<>();
		
		Query query = new Query();
		query.addCriteria(Criteria.where("category_info.category").regex("0015022500000000").and("user_info.type").is("uuid"));
		List<ClassCountMongoBean> classCountMongoList = mongoOperations.find(query, ClassCountMongoBean.class);
		System.out.println(classCountMongoList.size());
		for (ClassCountMongoBean classCountMongoBean : classCountMongoList) {
			set.add(classCountMongoBean.getUser_id());
		}
		
		Query query2 = new Query();
		query2.addCriteria(Criteria.where("category_info.category").regex("0015022720350000").and("user_info.type").is("uuid"));
		List<ClassCountMongoBean> classCountMongoList2 = mongoOperations.find(query2, ClassCountMongoBean.class);
		System.out.println(classCountMongoList2.size());
		for (ClassCountMongoBean classCountMongoBean : classCountMongoList2) {
			set.add(classCountMongoBean.getUser_id());
		}
		
		System.out.println(set.size());
			
	}
	
	public void process3() throws Exception {
		
		Set<String> set = new HashSet<>();
		
		Query query = new Query();
		query.addCriteria(Criteria.where("category_info.category").regex("0015022500000000").and("user_info.type").is("memid"));
		List<ClassCountMongoBean> classCountMongoList = mongoOperations.find(query, ClassCountMongoBean.class);
		System.out.println(classCountMongoList.size());
		for (ClassCountMongoBean classCountMongoBean : classCountMongoList) {
			set.add(classCountMongoBean.getUser_id());
		}
		
		Query query2 = new Query();
		query2.addCriteria(Criteria.where("category_info.category").regex("0015022720350000").and("user_info.type").is("memid"));
		List<ClassCountMongoBean> classCountMongoList2 = mongoOperations.find(query2, ClassCountMongoBean.class);
		System.out.println(classCountMongoList2.size());
		for (ClassCountMongoBean classCountMongoBean : classCountMongoList2) {
			set.add(classCountMongoBean.getUser_id());
		}
		
		System.out.println(set.size());
			
	}
	
	
	public void sex() throws Exception {
		
		Set<String> set = new HashSet<>();
		
		Query query = new Query();
		//3_memid_ad_click_F      180
		query.addCriteria(Criteria.where("user_info.sex").regex("F").and("user_info.type").is("memid"));
		List<ClassCountMongoBean> classCountMongoList = mongoOperations.find(query, ClassCountMongoBean.class);
		System.out.println(classCountMongoList.size());
		for (ClassCountMongoBean classCountMongoBean : classCountMongoList) {
			System.out.println(classCountMongoBean.getUser_id());
		}
		
		System.out.println("print OK OK OK");
		
//		Query query2 = new Query();
//		query2.addCriteria(Criteria.where("category_info.category").regex("0015022720350000").and("user_info.type").is("memid"));
//		List<ClassCountMongoBean> classCountMongoList2 = mongoOperations.find(query2, ClassCountMongoBean.class);
//		System.out.println(classCountMongoList2.size());
//		for (ClassCountMongoBean classCountMongoBean : classCountMongoList2) {
//			set.add(classCountMongoBean.getUser_id());
//		}
//		
//		System.out.println(set.size());
			
	}
	

	public void run() throws Exception {
		log.info("====CampaignJob.process() start====");

		File dir = new File(campaignlogPath);
		if (!dir.exists()) {
			log.error(dir.getPath() + " not exists");
			return;
		}

		String line = null;
		String[] lines = null;
		String memid = "";
		String uuid = "";
		String adClass = "";
		String age = "";
		String sex = "";
		String ipArea = "";
		String recordDate = "";

		java.io.File folder = new java.io.File(campaignlogPath);
		String[] fileList = folder.list();

		for (String fileName : fileList) {
			File file = new File(campaignlogPath + "/" + fileName);
			if (file.getName().endsWith(".err")) {
				file.delete();
				continue;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			while ((line = br.readLine()) != null) {
				lines = line.split(",");
				if (lines.length < 9) {
					continue;
				}

				memid = "";
				uuid = "";
				adClass = "";
				age = "";
				sex = "";
				ipArea = "";
				recordDate = "";
				
				memid = lines[0];
				uuid = lines[1];
				adClass = lines[2];
				age = lines[4];
				sex = lines[5];
				ipArea = lines[6];
				recordDate = lines[7];
				double w = 0;
				String id = StringUtils.isNotBlank(memid) ? memid : uuid;
				
				if(StringUtils.isBlank(id) || StringUtils.isBlank(adClass)){
					continue;
				}
				
				
				ClassCountLogBean classCountLogBean = new ClassCountLogBean();
				classCountLogBean.setMemid(memid);
				classCountLogBean.setUuid(uuid);
				classCountLogBean.setAdClass(adClass);
				classCountLogBean.setAge(age);
				classCountLogBean.setSex(sex);
				classCountLogBean.setIpArea(ipArea);
				classCountLogBean.setUserId(id);
				classCountLogBean.setW(w);
				classCountLogBean.setRecordDate(recordDate);
				classCountLogBean.setSource(BEHAVIOR);
				
				kafkaUtil.sendMessage("TEST", "", objectMapper.writeValueAsString(classCountLogBean));
			}
			
			br.close();
			file.delete();
			log.info(">>>>>> delete: " + file);
		}
		log.info("====CampaignJob.process() end====");
	}


	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
			KafkaUtil kafkaUtil = (KafkaUtil) ctx.getBean("kafkaUtil");
			
//			String tupleStr = "{'prodDis':'','trackId':'TAC20181210000000001','ecStockStatus':'','prodId':'10061','uuid':'01af18e9-b3db-4905-8252-b8646235a727','prodPrice':''}";
//			
//			JSONObject a = new JSONObject(tupleStr);
			
//			System.out.println(a);
			
//			kafkaUtil.sendMessage("sample", "", "96aef726-cc54-4d24-9e1b-8e1d91e29ea5");
			kafkaUtil.sendMessage("akb_ad_dmpapi_prd", "", "96aef726-cc54-4d24-9e1b-8e1d91e29ea5");
//			campaignJob.run();
//			campaignJob.process2();
//			campaignJob.process3();
//			campaignJob.sex();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
