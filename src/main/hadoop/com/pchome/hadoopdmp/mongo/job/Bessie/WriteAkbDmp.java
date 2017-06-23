//package com.pchome.kafkastorm.dmp.categorylog.process;
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
//import org.apache.log4j.Logger;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Scope;
//import org.springframework.data.mongodb.core.MongoOperations;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.stereotype.Component;
//
//import com.jayway.jsonpath.Configuration;
//import com.jayway.jsonpath.JsonPath;
//import com.pchome.kafkastorm.data.mongo.pojo.ClassCountMongoBean;
//import com.pchome.kafkastorm.dmp.categorylog.bean.ClassCountLogBean;
//import com.pchome.soft.depot.utils.DateFormatUtil;
//import com.pchome.soft.depot.utils.RestClientUtil;
//
//@Component
//@Scope("prototype")
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
//	protected static Logger log = Logger.getLogger("writeAkbDmp");
//	
//	@SuppressWarnings("unchecked")
//	public boolean process(ClassCountLogBean classCountLogBean) throws Exception{
//		
//		boolean processFlag = false;
//		ClassCountMongoBean classCountMongoBean = null;
//		Query query = new Query(Criteria.where("user_id").is(classCountLogBean.getUserId().trim()));
//		classCountMongoBean = mongoOperations.findOne(query, ClassCountMongoBean.class);
//		
//		if (classCountMongoBean == null) {
//			Map<String, Object> userInfo = new HashMap<String, Object>();
//			userInfo.put("type", classCountLogBean.getType());
//			if(classCountLogBean.getType().equals("memid")){
//				Map<String,String> sexAgeMap = callMemberApi(classCountLogBean.getUserId());
//				userInfo = processSexAge(userInfo,sexAgeMap);
//			}
//			
//			if(classCountLogBean.getType().equals("uuid")){
//				if(StringUtils.isNotBlank(classCountLogBean.getMemid()) && StringUtils.isNotBlank(classCountLogBean.getUuid())){
//					Query queryMemid = new Query(Criteria.where("user_id").is(classCountLogBean.getMemid().trim()));
//					ClassCountMongoBean classCountMongoBeanMemid = mongoOperations.findOne(queryMemid, ClassCountMongoBean.class);
//					userInfo = classCountMongoBeanMemid.getUser_info();
//					userInfo.put("type", "uuid");
//				}else{
//					Map<String,String> sexAgeMap = new HashMap<String,String>();
//					sexAgeMap.put("sex", classCountLogBean.getSex());
//					sexAgeMap.put("age", classCountLogBean.getAge());
//					
//					userInfo.put("male_w", "0");
//					userInfo.put("female_w", "0");
//					userInfo = processSexAge(userInfo,sexAgeMap);
//				}
//				
//			}
//			
//			Map<String, Object> categoryInfo = new HashMap<String, Object>();
//			categoryInfo.put("category", classCountLogBean.getAdClass());
//			categoryInfo.put("w", classCountLogBean.getW());
//			categoryInfo.put("ad_class_million_count", new Integer(0));
//			categoryInfo.put("ad_class_day_count", new Integer(1));
//			categoryInfo.put("update_date", classCountLogBean.getRecordDate());
//			
//			
//			ArrayList<String> sourceList = new ArrayList<String>();
//			sourceList.add(classCountLogBean.getSource());
//			categoryInfo.put("source", sourceList);
//
//			List<Map<String, Object>> categoryInfoList = new ArrayList<Map<String, Object>>();
//			categoryInfoList.add(categoryInfo);
//
//			classCountMongoBean = new ClassCountMongoBean();
//			classCountMongoBean.setUser_id(classCountLogBean.getUserId());
//			classCountMongoBean.setUser_info(userInfo);
//			classCountMongoBean.setCategory_info(categoryInfoList);
//			classCountMongoBean.setCreate_date(dateFormatUtil.getDateTemplate2().format(new Date()));
//		} else {
//			Set<String> set = new HashSet<String>();
//			// 加分類
//			if (!JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
//				double w = 0;
//				Map<String, Object> newCategoryInfo = new HashMap<String, Object>();
//				newCategoryInfo.put("w", w);
//				newCategoryInfo.put("category", classCountLogBean.getAdClass());
//				newCategoryInfo.put("ad_class_million_count", new Integer(0));
//				newCategoryInfo.put("ad_class_day_count", new Integer(1));
//				newCategoryInfo.put("update_date", classCountLogBean.getRecordDate());
//				
//				ArrayList<String> sourceList = new ArrayList<String>();
//				sourceList.add(classCountLogBean.getSource());
//				newCategoryInfo.put("source", sourceList);
//				classCountMongoBean.getCategory_info().add(newCategoryInfo);
//				classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
//			}else if (JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
//				// 分類已存在則更新時間,來源,adClass次數
//				for (Map<String, Object> categoryInfo : classCountMongoBean.getCategory_info()) {
//					if (categoryInfo.get("category").equals(classCountLogBean.getAdClass())) {
//						int adClassCount = Integer.parseInt(categoryInfo.get("ad_class_day_count").toString());
//						adClassCount = adClassCount + 1;
//						if(adClassCount == 10000000){
//							adClassCount = 0;
//							int millionCount =  Integer.parseInt(categoryInfo.get("ad_class_million_count").toString());
//							millionCount = millionCount + 1;
//							categoryInfo.put("ad_class_million_count", millionCount);
//							categoryInfo.put("ad_class_day_count", adClassCount);
//						}else{
//							categoryInfo.put("ad_class_day_count", adClassCount);
//						}
//						
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
//			
//			//記算性別分數
//			if(classCountLogBean.getType().equals("uuid")){
//				Map<String, Object> userInfo = classCountMongoBean.getUser_info();
//				Map<String,String> sexAgeMap = new HashMap<String,String>();
//				sexAgeMap.put("sex", classCountLogBean.getSex());
//				sexAgeMap.put("age", classCountLogBean.getAge());
//				userInfo = processSexAge(userInfo,sexAgeMap);
//				classCountMongoBean.setUser_info(userInfo);
//			}
//			
//		}
//
//		classCountMongoBean = episteMath(classCountMongoBean, classCountLogBean.getAdClass(), classCountLogBean.getRecordDate());
//		classCountMongoBean.setUpdate_date(dateFormatUtil.getDateTemplate2().format(new Date()));
//		mongoOperations.save(classCountMongoBean);
//		processFlag = true;
//		return processFlag;
//	}
//	
//	//處理性別權重
//	public Map<String,Object> processSexAge(Map<String,Object> userInfo,Map<String,String> sexAgeMap) throws Exception {
//		if(userInfo.get("type").equals("memid")){
//			if(sexAgeMap.get("sex").equals("M")){
//				userInfo.put("male_w",new Double(1));
//				userInfo.put("female_w",new Double(0));
//				userInfo.put("sex", "M");
//			}else if(sexAgeMap.get("sex").equals("F")){
//				userInfo.put("male_w",new Double(0));
//				userInfo.put("female_w",new Double(1));
//				userInfo.put("sex", "F");
//			}else {
//				userInfo.put("male_w",new Double(0));
//				userInfo.put("female_w",new Double(0));
//				userInfo.put("sex","");
//			}
//			userInfo.put("age", sexAgeMap.get("age"));
//		}
//		
//		if(userInfo.get("type").equals("uuid")){
//			String sex = StringUtils.isBlank(sexAgeMap.get("sex")) ? "0" : sexAgeMap.get("sex");
//			if(!sex.equals("0")){
//				double pExpv = Math.exp(-1 * 0.05);
//				double male_w;
//				double female_w;
//				if(sex.equals("M")){
//					male_w = Double.parseDouble(userInfo.get("male_w").toString());
//					double newMale_w = male_w + (1 / (1 + pExpv));
//					userInfo.put("male_w", newMale_w);
//				}
//				if(sex.equals("F")){
//					female_w = Double.parseDouble(userInfo.get("female_w").toString());
//					double newFemale_w = female_w + (1 / (1 + pExpv));
//					userInfo.put("female_w", newFemale_w);
//				}
//			}else{
//				userInfo.put("sex", "");
//			}
//			
//			if(Double.parseDouble(userInfo.get("male_w").toString()) > Double.parseDouble(userInfo.get("female_w").toString())){
//				userInfo.put("sex", "M");
//			}else if(Double.parseDouble(userInfo.get("male_w").toString()) < Double.parseDouble(userInfo.get("female_w").toString())){
//				userInfo.put("sex", "F");
//			}
//			userInfo.put("age", sexAgeMap.get("age"));
//		}
//		return userInfo;
//	}
//	
//	/**
//	 * 牛頓冷卻 新權重 = w * Math.exp(-0.1 * (1 * 0.1)); 新權重 = 上一次的權重 * Math.exp(-0.1 *
//	 * (天*0.1))
//	 * 
//	 * 邏輯迴歸線性增加公式 double pExpv = 0; pExpv = Math.exp(-1 * 0.05); 
//	 * new_w = w + (1 / (1 + pExpv)); 
//	 * 新權重 = 上一次的權重 + (1 / (1 + pExpv));
//	 * 
//	 */
//	public ClassCountMongoBean episteMath(ClassCountMongoBean classCountMongoBean, String adClass, String recodeDate) throws Exception {
//		List<Map<String, Object>> categoryInfoList = classCountMongoBean.getCategory_info();
//		for (Map<String, Object> categoryInfo : categoryInfoList) {
//			if (categoryInfo.get("category").equals(adClass)) {
//				double pExpv = Math.exp(-1 * 0.05);
//				double w = Double.valueOf(categoryInfo.get("w").toString());
//				double nw = w + (1 / (1 + pExpv));
//				categoryInfo.put("w", nw);
//			} else {
//				// 判斷當日是否更新過
//				SimpleDateFormat simpleDateFormat = dateFormatUtil.getDateTemplate();
//				Date startDate = simpleDateFormat.parse(categoryInfo.get("update_date").toString());
//				Date endDate = simpleDateFormat.parse(recodeDate);
//				int betweenDate = (int) ((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
//				if (betweenDate > 0) {
//					double w = Double.valueOf(categoryInfo.get("w").toString());
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
//		Map<String,String> map = new HashMap<String,String>();
//		if(JsonPath.using(jsonpathConfiguration).parse(result).read("stat") != null){
//			map.put("sex", "");
//			map.put("age", "");
//			return map;
//		}
//		
//		String sex = JsonPath.parse(result).read("sexuality");
//		String age = "";
//		if(StringUtils.isNotBlank(JsonPath.parse(result).read("birthday").toString())){
//			String birthday = JsonPath.parse(result).read("birthday").toString();
//			Date birthdayDate = sdf.parse(birthday);
//			age = String.valueOf(getAge(birthdayDate));
//		}
//		
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
//		return age;
//	}
//}
