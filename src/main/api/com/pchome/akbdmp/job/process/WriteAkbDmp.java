package com.pchome.akbdmp.job.process;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.job.bean.ClassCountLogBean;
import com.pchome.soft.depot.utils.DateFormatUtil;
import com.pchome.soft.depot.utils.RestClientUtil;

@Component
public class WriteAkbDmp {

	@Autowired
	private DateFormatUtil dateFormatUtil;
	
	@Autowired
	MongoOperations mongoOperations;
	
	@Autowired
	RestClientUtil restClientUtil;
	
	@Autowired
	private Configuration jsonpathConfiguration;
	
	public boolean process(ClassCountLogBean classCountLogBean) throws Exception{
		boolean processFlag = false;
		ClassCountMongoBean classCountMongoBean = null;
		Query query = new Query(Criteria.where("user_id").is(classCountLogBean.getUserId().trim()));
		classCountMongoBean = mongoOperations.findOne(query, ClassCountMongoBean.class);
//		ClassCountMongoBean classCountMongoBean = classCountService.findUserId(classCountLogBean.getUserId());
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
				classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
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
//		classCountService.saveOrUpdate(classCountMongoBean);
		mongoOperations.save(classCountMongoBean);
		processFlag = true;
		return processFlag;
		
	}


	/**
	 * 牛頓冷卻 新權重 = w * Math.exp(-0.1 * (1 * 0.1)); 新權重 = 上一次的權重 * Math.exp(-0.1 *
	 * (天*0.1))
	 * 
	 * 邏輯迴歸線性增加公式 double pExpv = 0; pExpv = Math.exp(-1 * 0.05); new_w = w + (1
	 * / (1 + pExpv)); 新權重 = 上一次的權重 + (1 / (1 + pExpv));
	 * 
	 */
	public ClassCountMongoBean episteMath(ClassCountMongoBean classCountMongoBean, String adClass, String recodeDate) throws Exception {
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
	
	public Map<String,String> callMemberApi() throws Exception{
		
		
		
		return null;
	}
	
}
