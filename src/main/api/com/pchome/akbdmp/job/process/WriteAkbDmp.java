//package com.pchome.akbdmp.job.process;
//
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.commons.lang.StringUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.mongodb.core.MongoOperations;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.stereotype.Component;
//
//import com.jayway.jsonpath.Configuration;
//import com.jayway.jsonpath.JsonPath;
//import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
//import com.pchome.akbdmp.job.bean.ClassCountLogBean;
//import com.pchome.soft.depot.utils.DateFormatUtil;
//import com.pchome.soft.depot.utils.RestClientUtil;
//
//@Component
//public class WriteAkbDmp {
//	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//	
//	@Autowired
//	private DateFormatUtil dateFormatUtil;
//	
//	@Autowired
//	MongoOperations mongoOperations;
//	
//	@Autowired
//	RestClientUtil restClientUtil;
//	
//	@Autowired
//	private Configuration jsonpathConfiguration;
//	
//	@SuppressWarnings("unchecked")
//	public boolean process(ClassCountLogBean classCountLogBean) throws Exception{
//		boolean processFlag = false;
//		ClassCountMongoBean classCountMongoBean = null;
//		Query query = new Query(Criteria.where("user_id").is(classCountLogBean.getUserId().trim()));
//		classCountMongoBean = mongoOperations.findOne(query, ClassCountMongoBean.class);
//		
//		if (classCountMongoBean == null) {
//			
//			Map<String, Object> userInfo = new HashMap<>();
//			userInfo.put("type", classCountLogBean.getType());
//			if(classCountLogBean.getType().equals("memid")){
//				Map<String,String> map = callMemberApi(classCountLogBean.getUserId());
//				userInfo.put("sex", map.get("sex").toString());
//				userInfo.put("age", map.get("age").toString());
//			}
//			
//			if(classCountLogBean.getType().equals("uuid")){
//				userInfo.put("sex", classCountLogBean.getSex());
//				userInfo.put("age", classCountLogBean.getAge());
//			}
//
//			Map<String, Object> categoryInfo = new HashMap<>();
//			categoryInfo.put("category", classCountLogBean.getAdClass());
//			categoryInfo.put("w", classCountLogBean.getW());
//			categoryInfo.put("update_date", classCountLogBean.getRecordDate());
//			
//			ArrayList<String> sourceList = new ArrayList<String>();
//			sourceList.add(classCountLogBean.getSource());
//			categoryInfo.put("source", sourceList);
//
//			List<Map<String, Object>> categoryInfoList = new ArrayList<>();
//			categoryInfoList.add(categoryInfo);
//
//			classCountMongoBean = new ClassCountMongoBean();
//			classCountMongoBean.setUser_id(classCountLogBean.getUserId());
//			classCountMongoBean.setUser_info(userInfo);
//			classCountMongoBean.setCategory_info(categoryInfoList);
//			classCountMongoBean.setCreate_date(classCountLogBean.getRecordDate());
//		} else {
//			Set<String> set = new HashSet<>();
//			// 加分類
//			if (!JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
//				double w = 0;
//				Map<String, Object> newCategoryInfo = new HashMap<String, Object>();
//				newCategoryInfo.put("w", w);
//				newCategoryInfo.put("ud", classCountLogBean.getRecordDate());
//				newCategoryInfo.put("category", classCountLogBean.getAdClass());
//				newCategoryInfo.put("update_date", classCountLogBean.getRecordDate());
//				
//				ArrayList<String> sourceList = new ArrayList<String>();
//				sourceList.add(classCountLogBean.getSource());
//				newCategoryInfo.put("source", sourceList);
//				classCountMongoBean.getCategory_info().add(newCategoryInfo);
//				classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
//			}
//
//			// 分類已存在則更新時間,來源
//			if (JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
//				for (Map<String, Object> categoryInfo : classCountMongoBean.getCategory_info()) {
//					if (categoryInfo.get("category").equals(classCountLogBean.getAdClass())) {
//						ArrayList<String> sourceList = (ArrayList<String>) categoryInfo.get("source");
//						set.addAll(sourceList);
//						set.add(classCountLogBean.getSource());
//						sourceList.clear();
//						sourceList = new ArrayList<String>(set);
//						categoryInfo.put("update_date", classCountLogBean.getRecordDate());
//						categoryInfo.put("source", sourceList);
//						break;
//					}
//				}
//			}
//		}
//
//		classCountMongoBean = episteMath(classCountMongoBean, classCountLogBean.getAdClass(), classCountLogBean.getRecordDate());
//		classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
//		mongoOperations.save(classCountMongoBean);
//		processFlag = true;
//		return processFlag;
//		
//	}
//
//
//	/**
//	 * 牛頓冷卻 新權重 = w * Math.exp(-0.1 * (1 * 0.1)); 新權重 = 上一次的權重 * Math.exp(-0.1 *
//	 * (天*0.1))
//	 * 
//	 * 邏輯迴歸線性增加公式 double pExpv = 0; pExpv = Math.exp(-1 * 0.05); new_w = w + (1
//	 * / (1 + pExpv)); 新權重 = 上一次的權重 + (1 / (1 + pExpv));
//	 * 
//	 */
//	public ClassCountMongoBean episteMath(ClassCountMongoBean classCountMongoBean, String adClass, String recodeDate) throws Exception {
//		List<Map<String, Object>> categoryInfoList = classCountMongoBean.getCategory_info();
//		for (Map<String, Object> categoryInfo : categoryInfoList) {
//			if (categoryInfo.get("category").equals(adClass)) {
//				double pExpv = Math.exp(-1 * 0.05);
//				double w = (double) categoryInfo.get("w");
//				double nw = w + (1 / (1 + pExpv));
//				categoryInfo.put("w", nw);
//			} else {
//				// 判斷當日是否更新過
//				SimpleDateFormat simpleDateFormat = dateFormatUtil.getDateTemplate();
//				Date startDate = simpleDateFormat.parse(categoryInfo.get("update_date").toString());
//				Date endDate = simpleDateFormat.parse(recodeDate);
//				int betweenDate = (int) ((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
//				if (betweenDate > 0) {
//					double w = (double) categoryInfo.get("w");
//					double nw = w * Math.exp(-0.1 * (betweenDate * 0.1));
//					categoryInfo.put("w", nw);
//					categoryInfo.put("update_date", recodeDate);
//				}
//			}
//		}
//		return classCountMongoBean;
//	}
//	
//	public Map<String,String> callMemberApi(String memid) throws Exception{
//		String result = restClientUtil.post("http://member.pchome.com.tw/findMemberInfo4ADAPI.html?ad_user_id="+memid, null);
//		String sex = JsonPath.parse(result).read("sexuality");
//		String age = "";
//		if(StringUtils.isNotBlank(JsonPath.parse(result).read("birthday").toString())){
//			String birthday = JsonPath.parse(result).read("birthday").toString();
//			Date birthdayDate = sdf.parse(birthday);
//			age = String.valueOf(getAge(birthdayDate));
//		}
//		Map<String,String> map = new HashMap<>();
//		map.put("sex", sex);
//		map.put("age", age);
//		return map;
//	}
//	
//	public int getAge(Date birthDay) {
//		Calendar cal = Calendar.getInstance();
//
//		if (cal.before(birthDay)) {
//			return -1;
//		}
//
//		int yearNow = cal.get(Calendar.YEAR);
//		int monthNow = cal.get(Calendar.MONTH)+1;
//		int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
//
//		cal.setTime(birthDay);
//		int yearBirth = cal.get(Calendar.YEAR);
//		int monthBirth = cal.get(Calendar.MONTH)+1;
//		int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);
//
//		int age = yearNow - yearBirth;
//
//		if (monthNow <= monthBirth) {
//			if (monthNow == monthBirth) {
//				if (dayOfMonthNow < dayOfMonthBirth) {
//					age--;
//				}
//			} else {
//				age--;
//			}
//		}
//
//		return age;
//	}
//}
