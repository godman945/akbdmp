//package test.bessie;
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
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.AnnotationConfigApplicationContext;
//import org.springframework.context.annotation.Scope;
//import org.springframework.data.authentication.UserCredentials;
//import org.springframework.data.mongodb.core.BulkOperations;
//import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
//import org.springframework.data.mongodb.core.MongoOperations;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.stereotype.Component;
//
//import com.jayway.jsonpath.Configuration;
//import com.jayway.jsonpath.JsonPath;
//import com.mongodb.BulkWriteOperation;
//import com.mongodb.Mongo;
//import com.mongodb.WriteConcern;
//import com.pchome.akbdmp.job.bean.ClassCountLogBean;
//import com.pchome.hadoopdmp.data.mongo.pojo.ClassCountMongoBean;
//import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
//import com.pchome.soft.util.DateFormatUtil;
//import com.pchome.soft.util.RestClientUtil;
//
//@Component
//@Scope("prototype")
//public class WriteAkbDmp {
//	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//	
//	@Autowired
//	private DateFormatUtil dateFormatUtil;
//	
////	@Autowired
//	private static MongoOperations writeUserDetailMongoOperations;
//	
//	@Autowired
//	private RestClientUtil restClientUtil;
//	
//	@Autowired
//	private Configuration jsonpathConfiguration;
//	
//	
//	protected static Logger log = Logger.getLogger("writeAkbDmp");
//	
//	@SuppressWarnings("unchecked")
//	public void process(ClassCountLogBean classCountLogBean) throws Exception{
//		//寫入正式機user_detail
//		MongoTemplate mongoTemplate = AdLogClassCount.newDBMongoTemplate;
//		this.writeUserDetailMongoOperations = mongoTemplate;
//		
//		//撈新結構是否有資料
//		ClassCountMongoBean classCountMongoBean = null;
//		Query query = new Query(Criteria.where("user_id").is(classCountLogBean.getUserId().trim()));
//		classCountMongoBean = writeUserDetailMongoOperations.findOne(query, ClassCountMongoBean.class);
//		
//		
//		//新結構沒資料
//		if (classCountMongoBean == null) {
//			double pExpv = Math.exp(-1 * 0.05);
//			double w = (1 / (1 + pExpv));
//			
//			Map<String, Object> userInfo = new HashMap<String, Object>();
//			userInfo.put("type", classCountLogBean.getType());
//			//如果uuid有memid資料，則在user_info加memid資料
//			userInfo.put("memid",classCountLogBean.getType().equals("uuid")? StringUtils.isNotBlank(classCountLogBean.getMemid())? classCountLogBean.getMemid() : "" : "" );
//			
//			//為會員 OR 為uuid
//			if((classCountLogBean.getType().equals("memid")) || (classCountLogBean.getType().equals("uuid"))){
//				//sex
//				List<Map<String, Object>> sexInfoDataList = new ArrayList<Map<String, Object>>();
//				Map<String, Object> sexInfoData = new HashMap<String, Object>();
//				List<String> sexSourceList = new ArrayList<>();
//				
//				//age
//				List<Map<String, Object>> ageInfoDataList = new ArrayList<Map<String, Object>>();
//				Map<String, Object> ageInfoData = new HashMap<String, Object>();
//				List<String> ageSourceList = new ArrayList<>();
//				
//				//判斷是否為真實個資
//				if (StringUtils.equals("1", classCountLogBean.getRealPersonalInfo())){
//					//sex
//					if (StringUtils.isNotBlank(classCountLogBean.getSex())){
//						userInfo.put("sex", classCountLogBean.getSex());
//						sexInfoData.put("w", -1);
//						sexInfoData.put("sex",classCountLogBean.getSex());
//						sexSourceList.add(classCountLogBean.getSource());
//						sexInfoData.put("source", sexSourceList);
//						sexInfoDataList.add(sexInfoData);
//						userInfo.put("sex_info", sexInfoDataList);
//					}
//					
//					//age
//					if (StringUtils.isNotBlank(classCountLogBean.getAge())){
//						userInfo.put("age", classCountLogBean.getAge());
//						ageInfoData.put("w", -1);
//						ageInfoData.put("age",classCountLogBean.getAge());
//						ageSourceList.add(classCountLogBean.getSource());
//						ageInfoData.put("source", ageSourceList);
//						ageInfoDataList.add(ageInfoData);
//						userInfo.put("age_info", ageInfoDataList);
//					}
//					
//				}
//				else{
//					//sex
//					if (StringUtils.isNotBlank(classCountLogBean.getSex())){
//						userInfo.put("sex", classCountLogBean.getSex());
//						sexInfoData.put("w", w);
//						sexInfoData.put("sex",classCountLogBean.getSex());
//						sexSourceList.add(classCountLogBean.getSource());
//						sexInfoData.put("source", sexSourceList);
//						sexInfoDataList.add(sexInfoData);
//						userInfo.put("sex_info", sexInfoDataList);
//					}
//					
//					//age
//					if (StringUtils.isNotBlank(classCountLogBean.getAge())){
//						userInfo.put("age", classCountLogBean.getAge());
//						ageInfoData.put("w", w);
//						ageInfoData.put("age",classCountLogBean.getAge());
//						ageSourceList.add(classCountLogBean.getSource());
//						ageInfoData.put("source", ageSourceList);
//						ageInfoDataList.add(ageInfoData);
//						userInfo.put("age_info", ageInfoDataList);
//					}
//				}
//					
//			}
//			
//			//塞age和sex以外的資料
//			Map<String, Object> categoryInfo = new HashMap<String, Object>();
//			categoryInfo.put("category", classCountLogBean.getAdClass());
//			categoryInfo.put("w", w);
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
//			classCountMongoBean.setCreate_date(classCountLogBean.getRecordDate());
//			classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
//			classCountMongoBean.setUser_info(userInfo);
//			classCountMongoBean.setCategory_info(categoryInfoList);
//			
//			writeUserDetailMongoOperations.save(classCountMongoBean);
//			
//		} 
//		else {//mongo有資料
//			
//			Set<String> set = new HashSet<String>();
//
//			if (!JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
//				// 加分類 //log的ad_class不存在在mongo
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
//				
////				Map<String, Object> userInfo = classCountMongoBean.getUser_info();
////				//記算性別分數
////				userInfo = processSexWeight(userInfo,null,classCountLogBean.getSex(),classCountLogBean.getSource());
////				//記算年齡分數
////				userInfo = processAgeWeight(userInfo,null,classCountLogBean.getAge(),classCountLogBean.getSource());
//				
//				classCountMongoBean = episteMath(classCountMongoBean, classCountLogBean.getAdClass(), classCountLogBean.getRecordDate());
//				writeUserDetailMongoOperations.save(classCountMongoBean);
//				
//			}//log的ad_class在mongo已存在
//			else if (JsonPath.using(jsonpathConfiguration).parse(classCountMongoBean.getCategory_info()).jsonString().contains(classCountLogBean.getAdClass())) {
//				// 分類已存在則更新時間,來源,adClass次數
//				for (Map<String, Object> categoryInfo : classCountMongoBean.getCategory_info()) {
//					if (categoryInfo.get("category").equals(classCountLogBean.getAdClass())) {
//						int adClassCount = Integer.parseInt(categoryInfo.get("ad_class_day_count").toString());
//						adClassCount = adClassCount + 1;
//						if(adClassCount == 1000000){
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
////				Map<String, Object> userInfo = classCountMongoBean.getUser_info();
////				//記算性別分數
////				userInfo = processSexWeight(userInfo,null,classCountLogBean.getSex(),classCountLogBean.getSource());
////				//記算年齡分數
////				userInfo = processAgeWeight(userInfo,null,classCountLogBean.getAge(),classCountLogBean.getSource());
//				
//				classCountMongoBean.setUpdate_date(classCountLogBean.getRecordDate());
//				classCountMongoBean = episteMath(classCountMongoBean, classCountLogBean.getAdClass(), classCountLogBean.getRecordDate());
//				writeUserDetailMongoOperations.save(classCountMongoBean);
//			}
//			
//		}
//		
//	}
//	
//	/*
//	 * 處理來源
//	 * return:userInfo
//	 * */
//	public Map<String,Object> processResource(Map<String,Object> ageSexMapData,String source) throws Exception {
//		if(StringUtils.isNotBlank(source)){
//			if(ageSexMapData.get("source") == null){
//				List<String> sourceList = new ArrayList<>();
//				sourceList.add(source);
//				ageSexMapData.put("source", sourceList);
//			}else{
//				List<String> sourceList = (List<String>) ageSexMapData.get("source");
//				Set<String> set = new HashSet<String>();
//				set.addAll(sourceList);
//				set.add(source);
//				sourceList.clear();
//				sourceList = new ArrayList<String>(set);
//				ageSexMapData.put("source", sourceList);
//			}
//		}
//		return ageSexMapData;
//	}
//	
//	
//	//處理性別權重
//	public Map<String,Object> processSexWeight(Map<String,Object> userInfo,String apiSex,String logSex,String source) throws Exception {
//		if(userInfo.get("type").equals("memid") && StringUtils.isNotBlank(apiSex)){
//			if(userInfo.get("sex_info") == null){
//				List<Map<String, Object>> sexInfoDataList = new ArrayList<Map<String, Object>>();
//				Map<String, Object> sexInfoData = new HashMap<String, Object>();
//				sexInfoData.put("sex", apiSex);
//				sexInfoData.put("w", -1);
//				sexInfoDataList.add(sexInfoData);
//				userInfo.put("sex_info", sexInfoDataList);
//				processResource(sexInfoData,source);
//			}
//		}
//		
//		if(userInfo.get("type").equals("uuid") && StringUtils.isNotBlank(logSex)){
//			double pExpv = Math.exp(-1 * 0.05);
//			if(userInfo.get("sex_info") == null){
//				if(StringUtils.isNotBlank(logSex)){
//					List<Map<String, Object>> sexInfoDataList = new ArrayList<Map<String, Object>>();
//					Map<String, Object> sexInfoData = new HashMap<String, Object>();
//					double sex_w = (1 / (1 + pExpv));
//					sexInfoData.put("sex", logSex);
//					sexInfoData.put("w", sex_w);
//					sexInfoDataList.add(sexInfoData);
//					userInfo.put("sex_info", sexInfoDataList);
//				}
//			}else{
//				List<Map<String, Object>> sexInfoDataList = (List<Map<String, Object>>) userInfo.get("sex_info");
//				if(JsonPath.using(jsonpathConfiguration).parse(userInfo.get("sex_info")).jsonString().contains(logSex)){
//					for (Map<String, Object> map : sexInfoDataList) {
//						if(map.get("sex").equals(logSex)){
//							double w = Double.valueOf(map.get("w").toString());
//							if(w == -1){
//								return userInfo;
//							}
//							double sex_w = w + (1 / (1 + pExpv));
//							map.put("w", sex_w);
//							processResource(map,source);
//							break;
//						}
//					}
//				}else{
//					Map<String, Object> sexInfoData = new HashMap<String, Object>();
//					double sex_w = (1 / (1 + pExpv));
//					sexInfoData.put("sex", logSex);
//					sexInfoData.put("w", sex_w);
//					sexInfoDataList.add(sexInfoData);
//					userInfo.put("sex_info", sexInfoDataList);
//					processResource(sexInfoData,source);
//				}
//			}
//		}
//		
//		//取得sex_info的array，然後比大小，最大的為最外層的sex
//		String sex = "";
//		double w = 0;
//		List<Map<String, Object>> sexInfoDataList = (List<Map<String, Object>>) userInfo.get("sex_info");
//		if(sexInfoDataList != null){
//			for (Map<String, Object> map : sexInfoDataList) {
//				if(StringUtils.isNotBlank(sex)){
//					double sex_w = Double.valueOf(map.get("w").toString());
//					if(sex_w == -1){
//						w = sex_w;
//						sex = map.get("sex").toString();
//						break;
//					}else if(sex_w > w){
//						w = sex_w;
//						sex = map.get("sex").toString();
//					}
//				}else{
//					w = Double.valueOf(map.get("w").toString());
//					sex = map.get("sex").toString();
//				}
//			}
//		}
//		userInfo.put("sex", sex);
//		return userInfo;
//	}
//	
//	/*
//	 * 處理年齡權重
//	 * return:userInfo
//	 * */
//	public Map<String,Object> processAgeWeight(Map<String,Object> userInfo,String apiAge,String logAge,String source) throws Exception {
//		if(userInfo.get("type").equals("memid")){
//			if(userInfo.get("age_info") == null){
//				if(StringUtils.isNotBlank(apiAge)){
//					List<Map<String, Object>> ageInfoDataList = new ArrayList<Map<String, Object>>();
//					Map<String, Object> ageInfoData = new HashMap<String, Object>();
//					ageInfoData.put("age", apiAge);
//					ageInfoData.put("w", -1);
//					ageInfoDataList.add(ageInfoData);
//					userInfo.put("age_info", ageInfoDataList);
//					processResource(ageInfoData,source);
//				}
//			}
//		}
//		
//		if(userInfo.get("type").equals("uuid") && StringUtils.isNotBlank(logAge)){
//			double pExpv = Math.exp(-1 * 0.05);
//			if(userInfo.get("age_info") == null){
//				if(StringUtils.isNotBlank(logAge)){
//					List<Map<String, Object>> ageInfoDataList = new ArrayList<Map<String, Object>>();
//					Map<String, Object> ageInfoData = new HashMap<String, Object>();
//					double age_w = (1 / (1 + pExpv));
//					ageInfoData.put("age", logAge);
//					ageInfoData.put("w", age_w);
//					ageInfoDataList.add(ageInfoData);
//					userInfo.put("age_info", ageInfoDataList);
//					processResource(ageInfoData,source);
//				}
//			}else{
//				List<Map<String, Object>> ageInfoDataList = (List<Map<String, Object>>) userInfo.get("age_info");
//				if(JsonPath.using(jsonpathConfiguration).parse(userInfo.get("age_info")).jsonString().contains(logAge)){
//					for (Map<String, Object> map : ageInfoDataList) {
//						if(map.get("age").equals(logAge)){
//							double w = Double.valueOf(map.get("w").toString());
//							if(w == -1){
//								return userInfo;
//							}
//							double sex_w = w + (1 / (1 + pExpv));
//							map.put("w", sex_w);
//							processResource(map,source);
//							break;
//						}
//					}
//				}else{
//					Map<String, Object> ageInfoData = new HashMap<String, Object>();
//					double age_w = (1 / (1 + pExpv));
//					ageInfoData.put("age", logAge);
//					ageInfoData.put("w", age_w);
//					ageInfoDataList.add(ageInfoData);
//					processResource(ageInfoData,source);
//				}
//			}
//		}
//		
//		String age = "";
//		double w = 0;
//		List<Map<String, Object>> ageInfoDataList = (List<Map<String, Object>>) userInfo.get("age_info");
//		if(ageInfoDataList != null){
//			for (Map<String, Object> map : ageInfoDataList) {
//				if(StringUtils.isNotBlank(age)){
//					double age_w = Double.valueOf(map.get("w").toString());
//					if(age_w == -1){
//						w = age_w;
//						age = map.get("age").toString();
//						break;
//					}else if(age_w > w){
//						w = age_w;
//						age = map.get("age").toString();
//					}
//				}else{
//					w = Double.valueOf(map.get("w").toString());
//					age = map.get("age").toString();
//				}
//			}
//		}
//		userInfo.put("age", age);
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
//					double nw = w * Math.exp(- 0.1 * (betweenDate * 0.1));
//					if(nw<=0){//牛頓冷卻 到負值  ，直接給0
//						nw=0;
//					}
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
//	
////	public static void main(String args[]) throws Exception{
////		System.setProperty("spring.profiles.active", "local");
////		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
////		WriteAkbDmp writeAkbDmp = ctx.getBean(WriteAkbDmp.class);
////		
////		ClassCountLogBean classCountLogBean = new ClassCountLogBean();
////		classCountLogBean.setUserId("863a0eb3-e02a-4372-93b5-0c020b2ff38f");
////		classCountLogBean.setAdClass("0016023020711111");
////		classCountLogBean.setSource("24h");
////		classCountLogBean.setRecordDate("2017-07-17");
////		
////		writeAkbDmp.process(classCountLogBean);
////	}
//	
//}
