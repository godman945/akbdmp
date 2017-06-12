package com.pchome.akbdmp.job.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.job.bean.ClassCountLogBean;
import com.pchome.akbdmp.mongo.db.service.classcount.IClassCountService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.DateFormatUtil;

@Component
public class CampaignJob {

	Log log = LogFactory.getLog(this.getClass());

	@Value("${save.path.campaignlog}")
	private String campaignlogPath;

	@Autowired
	private IClassCountService classCountService;

	@Autowired
	private Configuration jsonpathConfiguration;

	@Autowired
	private DateFormatUtil dateFormatUtil;

	@Autowired
	MongoOperations mongoOperations;

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
		String memid = null;
		String uuid = null;
		String adClass = null;
		String age = null;
		String sex = null;
		String ipArea = null;
		String recordDate = null;

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

				memid = lines[0];
				uuid = lines[1];
				adClass = lines[2];
				age = lines[4];
				sex = lines[5];
				ipArea = lines[6];
				recordDate = lines[7];
				double w = 0;
				String id = StringUtils.isNotBlank(memid) ? memid : uuid;
				String type = StringUtils.isNotBlank(memid) ? "memid" : "uuid";
				
				ClassCountLogBean classCountLogBean = new ClassCountLogBean();
				classCountLogBean.setMemid(memid);
				classCountLogBean.setUuid(uuid);
				classCountLogBean.setAdClass(adClass);
				classCountLogBean.setAge(age);
				classCountLogBean.setSex(sex);
				classCountLogBean.setIpArea(ipArea);
				classCountLogBean.setUserId(id);
				classCountLogBean.setType(type);
				classCountLogBean.setW(w);
				classCountLogBean.setUserId(id);
				classCountLogBean.setRecordDate(recordDate);
				classCountLogBean.setSource("campaign");
				process(classCountLogBean);
			}
			
			br.close();
			file.delete();
			log.info(">>>>>> delete: " + file);
		}
		log.info("====CampaignJob.process() end====");
	}

	public void process(ClassCountLogBean classCountLogBean) throws Exception{
		ClassCountMongoBean classCountMongoBean = classCountService.findUserId(classCountLogBean.getUserId());
		if (classCountMongoBean == null) {
			Map<String, Object> userInfo = new HashMap<>();
			userInfo.put("type", classCountLogBean.getType());
			userInfo.put("sex", classCountLogBean.getSex());
			userInfo.put("age", classCountLogBean.getAge());

			Map<String, Object> categoryInfo = new HashMap<>();
			categoryInfo.put("category", classCountLogBean.getAdClass());
			categoryInfo.put("w", classCountLogBean.getW());
			categoryInfo.put("update_date", classCountLogBean.getRecordDate());
			categoryInfo.put("source", classCountLogBean.getSource());

			List<Map<String, Object>> categoryInfoList = new ArrayList<>();
			categoryInfoList.add(categoryInfo);

			classCountMongoBean = new ClassCountMongoBean();
			classCountMongoBean.setUser_id(classCountLogBean.getUserId());
			classCountMongoBean.setUser_info(userInfo);
			classCountMongoBean.setCategory_info(categoryInfoList);
			classCountMongoBean.setCreate_date(classCountLogBean.getRecordDate());
		} else {
			// 加分類
			if (!JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
				double w = 0;
				Map<String, Object> categoryInfo = new HashMap<>();
				categoryInfo.put("w", w);
				categoryInfo.put("ud", classCountLogBean.getRecordDate());
				categoryInfo.put("source", classCountLogBean.getSource());
				categoryInfo.put("category", classCountLogBean.getAdClass());
				categoryInfo.put("update_date", classCountLogBean.getRecordDate());
				List<Map<String, Object>> categoryInfoList = classCountMongoBean.getCategory_info();
				categoryInfoList.add(categoryInfo);
				classCountMongoBean.setCreate_date(classCountLogBean.getRecordDate());
			}

			// 分類已存在則更新時間
			if (JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
				for (Map<String, Object> categoryInfo : classCountMongoBean.getCategory_info()) {
					if (categoryInfo.get("category").equals(classCountLogBean.getAdClass())) {
						categoryInfo.put("update_date", classCountLogBean.getRecordDate());
						break;
					}
				}
			}
		}

		classCountMongoBean = episteMath(classCountMongoBean, classCountLogBean.getAdClass(), classCountLogBean.getRecordDate());
		classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
		classCountService.saveOrUpdate(classCountMongoBean);
	}


	/**
	 * 牛頓冷卻 新權重 = w * Math.exp(-0.1 * (1 * 0.1)); 新權重 = 上一次的權重 * Math.exp(-0.1 *
	 * (天*0.1))
	 * 
	 * 邏輯迴歸線性增加公式 double pExpv = 0; pExpv = Math.exp(-1 * 0.05); new_w = w + (1
	 * / (1 + pExpv)); 新權重 = 上一次的權重 + (1 / (1 + pExpv));
	 * 
	 */
	public ClassCountMongoBean episteMath(ClassCountMongoBean classCountMongoBean, String adClass, String recodeDate)
			throws Exception {
		List<Map<String, Object>> categoryInfoList = classCountMongoBean.getCategory_info();
		for (Map<String, Object> categoryInfo : categoryInfoList) {
			if (categoryInfo.get("category").equals(adClass)) {
				double pExpv = Math.exp(-1 * 0.05);
				double w = (double) categoryInfo.get("w");
				double nw = w + (1 / (1 + pExpv));
				categoryInfo.put("w", nw);
			} else {
				// 判斷當日是否更新過
				SimpleDateFormat simpleDateFormat = dateFormatUtil.getDateTemplate();
				Date startDate = simpleDateFormat.parse(categoryInfo.get("update_date").toString());
				Date endDate = simpleDateFormat.parse(recodeDate);
				int betweenDate = (int) ((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
				if (betweenDate > 0) {
					double w = (double) categoryInfo.get("w");
					double nw = w * Math.exp(-0.1 * (betweenDate * 0.1));
					categoryInfo.put("w", nw);
					categoryInfo.put("update_date", recodeDate);
				}
			}
		}

		return classCountMongoBean;
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
			CampaignJob campaignJob = ctx.getBean(CampaignJob.class);
			campaignJob.run();
			// campaignJob.process2();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
