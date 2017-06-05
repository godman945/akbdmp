package com.pchome.akbdmp.job.campaign;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.mongo.db.service.classcount.IClassCountService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

import net.minidev.json.JSONObject;

@Component
public class CampaignJob {

	Log log = LogFactory.getLog(this.getClass());

	@Value("${save.path.campaignlog}")
	private String campaignlogPath;

	@Autowired
	private IClassCountService classCountService;

	@Autowired
	private Configuration jsonpathConfiguration;

	private final static String BEHAVIOR = "campaign";
	
	public void process2() throws Exception {
		ClassCountMongoBean classCountMongoBean = classCountService.findUserId("ebe21d8e-8371-4cdc-b05c-84cfbdca55f3");
		System.out.println(classCountMongoBean.get_id());
	}

	public void process() throws Exception {
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
			File file = new File(campaignlogPath+"/"+fileName);
			if(file.getName().endsWith(".err")){
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
				
				ClassCountMongoBean classCountMongoBean = classCountService.findUserId(id);
				if (classCountMongoBean == null) {
					net.minidev.json.JSONObject obj = new JSONObject();
					Map<String, Object> userInfo = new HashMap<>();
					userInfo.put("type", type);
					userInfo.put("sex", sex);
					userInfo.put("age", age);

					Map<String, Object> newCategoryDetail = new HashMap<>();
					newCategoryDetail.put("w", w);
					newCategoryDetail.put("ud", recordDate);
					newCategoryDetail.put("source", BEHAVIOR);

					Map<String, Object> category = new HashMap<>();
					category.put(adClass, newCategoryDetail);

					obj.put("userInfo", userInfo);
					obj.put("catagoryInfo", category);
					obj.put("user_id", id);

					classCountMongoBean = new ClassCountMongoBean();
					classCountMongoBean.setUser_id(id);
					classCountMongoBean.setUser_info(userInfo);
					classCountMongoBean.setCategory_info(category);
					classCountService.saveOrUpdate(classCountMongoBean);
				} else {
						// 加分類
						if (!JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(adClass)) {
							w = 0;
							Map<String, Object> newCategoryDetail = new HashMap<>();
							newCategoryDetail.put("w", w);
							newCategoryDetail.put("ud", recordDate);
							newCategoryDetail.put("source", ipArea);

							classCountMongoBean.getCategory_info().put(adClass, newCategoryDetail);
							classCountService.saveOrUpdate(classCountMongoBean);

						}
				}
				
				classCountMongoBean = episteMath(classCountMongoBean, adClass);
				classCountService.saveOrUpdate(classCountMongoBean);

			}
			br.close();
			log.info(">>>>>> delete: " + file);
			file.delete();
		}
		log.info("====CampaignJob.process() end====");
	}

	/**
	 * 牛頓冷卻 新權重 = w * Math.exp(-0.1 * (1*0.1));
	 * 
	 * 邏輯迴歸線性增加公式 double pExpv = 0; pExpv = Math.exp(-1 * 0.05); new_w = w + (1
	 * / (1 + pExpv)); 新權重 = 上一次的權重 + (1 / (1 + pExpv));
	 */
	public ClassCountMongoBean episteMath(ClassCountMongoBean classCountMongoBean, String adClass) throws Exception {

		Map<String, Object> categoryInfo = classCountMongoBean.getCategory_info();

		for (Entry<String, Object> entry : categoryInfo.entrySet()) {
			if (entry.getKey().equals(adClass)) {

				double pExpv = Math.exp(-1 * 0.05);
				double w = JsonPath.using(jsonpathConfiguration).parse(entry.getValue()).read("w");
				double nw = w + (1 / (1 + pExpv));

				Map<String, Object> newCategoryDetail3 = (Map<String, Object>) entry.getValue();
				newCategoryDetail3.put("w", nw);
			} else {
				double w = JsonPath.using(jsonpathConfiguration).parse(entry.getValue()).read("w");
				double nw = w * Math.exp(-0.1 * (1 * 0.1));
				Map<String, Object> newCategoryDetail3 = (Map<String, Object>) entry.getValue();
				newCategoryDetail3.put("w", nw);
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
			campaignJob.process();
			// campaignJob.process2();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
