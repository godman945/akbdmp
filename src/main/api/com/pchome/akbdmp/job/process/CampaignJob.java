package com.pchome.akbdmp.job.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.job.bean.ClassCountLogBean;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
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
		// 593a7ff5e4b07296c3205a27
		Query query = new Query();
		query.addCriteria(Criteria.where("category_info.category").regex("0012000000000000"));
		List<ClassCountMongoBean> classCountMongoList = mongoOperations.find(query, ClassCountMongoBean.class);
		System.out.println(classCountMongoList.size());
		for (ClassCountMongoBean classCountMongoBean : classCountMongoList) {
			System.out.println(classCountMongoBean.get_id());
		}
//		kafkaUtil.sendMessage("TEST", "", "123");
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


	@SuppressWarnings("resource")
	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
			CampaignJob campaignJob = ctx.getBean(CampaignJob.class);
			campaignJob.run();
//			 campaignJob.process2();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
