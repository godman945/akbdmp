package com.pchome.hadoopdmp.mapreduce.job.component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper.combinedValue;
import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;

public class PersonalInfoComponent {
	
	Log log = LogFactory.getLog("PersonalInfoComponent");
	
	private static DBCollection dBCollection;
	private static String memid = "";
	private static String category = "";
	private static DBObject dbObject;
	private static String userInfoStr;
	private static String msex = "";
	private static String mage = "";
	private static Map<String, Map<String, String>> sexAgeInfoMap = new HashMap<String, Map<String,String>>();
	private static StringBuffer url = new StringBuffer();
	private static String prsnlData = "";
	
	private static int count = 0;
	// 處理個資元件
	public net.minidev.json.JSONObject processPersonalInfo(net.minidev.json.JSONObject dmpJSon ,DBCollection dbCollectionUser) throws Exception {
		this.userInfoStr = "";
		this.dBCollection = dbCollectionUser;
		this.memid = dmpJSon.getAsString("memid");
		this.category = dmpJSon.getAsString("category");
		dbObject = null;
		// 如有memid資料，先查mongo，再撈會員中心查個資
		if(sexAgeInfoMap.containsKey(dmpJSon.getAsString("uuid")+"<PCHOME>"+memid)) {
			Map<String, String> personalInfoMap = sexAgeInfoMap.get(dmpJSon.getAsString("uuid")+"<PCHOME>"+memid);
			count = count + 1 ;
			log.info(">>>>>count:"+count);
			
			
		}else {
			if (StringUtils.isNotBlank(memid)) {
				log.info(">>>>>>memid:"+memid);
				dbObject = queryUserDetail(memid);
				msex = "";
				mage = "";
				Map<String, String> memberInfoMapApi = null;
				if (dbObject != null) {
					userInfoStr = dbObject.get("user_info").toString();
					// mongo user_detail舊資料中有無mage、msex
					if ((!userInfoStr.contains("mage")) || (!userInfoStr.contains("msex"))){
						memberInfoMapApi = findMemberInfoAPI(memid);
						msex = (String) memberInfoMapApi.get("msex");
						mage = (String) memberInfoMapApi.get("mage");
						//更新user資料
						updateUserDetail(memid,msex,mage);
						if(msex.equals("NA")) {
							dmpJSon.put("sex", "");
							memberInfoMapApi.put("msex", "");
						}else {
							dmpJSon.put("sex", msex);
						}
						if(mage.equals("NA")) {
							dmpJSon.put("age", "");
							memberInfoMapApi.put("mage", "");
						}else {
							dmpJSon.put("age", mage);
						}
					}
						
					if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
						dmpJSon.put("personal_info_api_classify", "Y");
					} else {
						dmpJSon.put("personal_info_api_classify", "N");
					}
				}else {
					// mongo尚未新增user_detail，直接新增一筆mongo資料，塞會員中心打回來的性別、年齡(空的轉成NA寫入)
					memberInfoMapApi = findMemberInfoAPI(memid);
					msex = (String) memberInfoMapApi.get("msex");
					mage = (String) memberInfoMapApi.get("mage");
					//新增user
					insertUserDetail(memid,msex,mage);
					if(msex.equals("NA")) {
						dmpJSon.put("sex", "");
						memberInfoMapApi.put("msex", "");
					}else {
						dmpJSon.put("sex", msex);
					}
					if(mage.equals("NA")) {
						dmpJSon.put("age", "");
						memberInfoMapApi.put("mage", "");
					}else {
						dmpJSon.put("age", mage);
					}
					
					if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
						dmpJSon.put("personal_info_api_classify", "Y");
					} else {
						dmpJSon.put("personal_info_api_classify", "N");
					}
				}
				dmpJSon.put("sex_source","member_api");
				dmpJSon.put("age_source","member_api");
				
				sexAgeInfoMap.put(dmpJSon.getAsString("uuid")+"<PCHOME>"+memid, memberInfoMapApi);
				
				log.info(">>>>>>memberInfoMapApi:"+memberInfoMapApi);
				
			}else {
				
				log.info(">>>>>>category:"+dmpJSon.getAsString(category));
				
				//處理個資推估
				Map<String, String> forecastPersonalInfoMap = processForecastPersonalInfo(dmpJSon,dmpJSon.getAsString(category));
				String sex = forecastPersonalInfoMap.get("msex");
				String age = forecastPersonalInfoMap.get("mage");
				if(StringUtils.isBlank(sex)) {
					dmpJSon.put("sex", "");
				}else {
					dmpJSon.put("sex", sex);
				}
				if(StringUtils.isBlank(age)) {
					dmpJSon.put("age", "");
				}else {
					dmpJSon.put("age", age);
				}
				dmpJSon.put("sex_source", StringUtils.equals(sex, "") ? "" : "excel");
				dmpJSon.put("age_source", StringUtils.equals(age, "") ? "" : "excel");
				if ((!StringUtils.equals(age, "")) && (!StringUtils.equals(sex, ""))) {
					dmpJSon.put("personal_info_classify", "Y");
				} else {
					dmpJSon.put("personal_info_classify", "N");
				}
				sexAgeInfoMap.put(dmpJSon.getAsString("uuid")+"<PCHOME>"+memid, forecastPersonalInfoMap);
				
				log.info(">>>>>>forecastPersonalInfoMap:"+forecastPersonalInfoMap);
			}
		}
		
		
		
		
		
		
//		// 如有memid資料，先查mongo，再撈會員中心查個資
//		// 撈回mongo為NA也算已打過會員中心API，不再重打會員中心api
//		if (StringUtils.isNotBlank(memid)) {
//			dbObject = queryUserDetail(memid);
//			msex = "";
//			mage = "";
//			if (dbObject != null) {
//				userInfoStr = dbObject.get("user_info").toString();
//				// mongo user_detail舊資料中有無mage、msex
//				 if ((!userInfoStr.contains("mage")) || (!userInfoStr.contains("msex"))){
//					memberInfoMapApi = findMemberInfoAPI(memid);
//					msex = (String) memberInfoMapApi.get("msex");
//					mage = (String) memberInfoMapApi.get("mage");
//					//更新user資料
//					updateUserDetail(memid,msex,mage);
//					dmpJSon.put("sex", "");
//					dmpJSon.put("age", "");
//					if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
//						dmpJSon.put("personal_info_api_classify", "Y");
//					} else {
//						dmpJSon.put("personal_info_api_classify", "N");
//					}
//				}else{
//					// mongodb已有資料就跳過,包括NA (mongo user_detail結構中已有mage和msex)
//					dmpJSon.put("sex", "");
//					dmpJSon.put("age", "");
//					dmpJSon.put("personal_info_api_classify", "Y");
//				}
//			} else {
//				// mongo尚未新增user_detail，直接新增一筆mongo資料，塞會員中心打回來的性別、年齡(空的轉成NA寫入)
//				memberInfoMapApi = findMemberInfoAPI(memid);
//				msex = (String) memberInfoMapApi.get("msex");
//				mage = (String) memberInfoMapApi.get("mage");
//				//新增user
//				insertUserDetail(memid,msex,mage);
//				dmpJSon.put("msex", "null");
//				dmpJSon.put("mage", "null");
//				if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
//					dmpJSon.put("personal_info_api_classify", "Y");
//				} else {
//					dmpJSon.put("personal_info_api_classify", "N");
//				}
//			}
//		}
//		
//		//如果raw data就有推估的age、sex，即PersonalInfo已被分類
//		if ((!StringUtils.equals(dmpJSon.getAsString("sex"), "null")) && (!StringUtils.equals((dmpJSon.getAsString("age")), "null")) ){
//			dmpJSon.put("personal_info_classify", "Y");
//			dmpJSon.put("sex_source",dmpJSon.getAsString("dmp_source"));
//			dmpJSon.put("age_source",dmpJSon.getAsString("dmp_source"));
//			return dmpJSon;
//		}
//		//處理個資推估
//		processForecastPersonalInfo(dmpJSon,category);
		
		return dmpJSon;
		
		
		
		
		
		
		
		
		
//		this.dBCollection= mongoOperations.getCollection("user_detail");
//		
//		String memid = dmpDataBean.getMemid();
//		String category = dmpDataBean.getCategory();
//		
//		// 如有memid資料，先查mongo，再撈會員中心查個資
//		// 撈回mongo為NA也算已打過會員中心API，不再重打會員中心api
//		if ((StringUtils.isNotBlank(memid)) && (!memid.equals("null"))) {
//			DBObject dbObject = queryUserDetail(memid);
//			
//			String msex = "";
//			String mage = "";
//			if (dbObject != null) {
//				String userInfoStr = dbObject.get("user_info").toString();
//				
//				// mongo user_detail舊資料中有無mage、msex
//				 if ( (!userInfoStr.contains("mage")) || (!userInfoStr.contains("msex")) ){
//					Map<String, Object> memberInfoMapApi = findMemberInfoAPI(memid);
//					msex = (String) memberInfoMapApi.get("msex");
//					mage = (String) memberInfoMapApi.get("mage");
//					//更新user資料
//					updateUserDetail(memid,msex,mage);
//					dmpDataBean.setMsex("null");
//					dmpDataBean.setMage("null");
//					if ( (!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA")) ) {
//						dmpDataBean.setPersonalInfoApiClassify("Y");
//					} else {
//						dmpDataBean.setPersonalInfoApiClassify("N");
//					}
//				}else{
//					// mongodb已有資料就跳過,包括NA (mongo user_detail結構中已有mage和msex)
//					dmpDataBean.setMsex("null");
//					dmpDataBean.setMage("null");
//					dmpDataBean.setPersonalInfoApiClassify("Y");
//				}
//			} else {
//				// mongo尚未新增user_detail，直接新增一筆mongo資料，塞會員中心打回來的性別、年齡(空的轉成NA寫入)
//				Map<String, Object> memberInfoMapApi = findMemberInfoAPI(memid);
//				msex = (String) memberInfoMapApi.get("msex");
//				mage = (String) memberInfoMapApi.get("mage");
//				//新增user
//				insertUserDetail(memid,msex,mage);
//				dmpDataBean.setMsex("null");
//				dmpDataBean.setMage("null");
//				if ( (!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA")) ) {
//					dmpDataBean.setPersonalInfoApiClassify("Y");
//				} else {
//					dmpDataBean.setPersonalInfoApiClassify("N");
//				}
//			}
//		}
//		
//		//如果raw data就有推估的age、sex，即PersonalInfo已被分類
//		if ( (!StringUtils.equals(dmpDataBean.getSex(), "null")) && (!StringUtils.equals(dmpDataBean.getAge(), "null")) ){
//			dmpDataBean.setPersonalInfoClassify("Y");
//			dmpDataBean.setSexSource(dmpDataBean.getSource());
//			dmpDataBean.setAgeSource(dmpDataBean.getSource());
//			return dmpDataBean;
//		}
//		//處理個資推估
//		dmpDataBean = processForecastPersonalInfo(dmpDataBean,category);
//		
//		return dmpDataBean;
	}
	

	
	public Map<String, String> processForecastPersonalInfo(net.minidev.json.JSONObject dmpJSon, String category) throws Exception {
		// 讀取ClsfyGndAgeCrspTable.txt做age、sex個資推估
		return forecastPersonalInfo(category);
	}
	
	public DBObject queryUserDetail(String memid) throws Exception {
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("user_id", memid));
		andQuery.put("$and", obj);
		DBObject dbObject =  dBCollection.findOne(andQuery);
		return dbObject;
	}
	
	public void updateUserDetail(String memid,String msex,String mage) throws Exception {
		DBObject updateCondition = new BasicDBObject();
		updateCondition.put("user_id", memid);
		DBObject updatedValue = new BasicDBObject();
		updatedValue.put("user_info", new BasicDBObject("msex", msex).append("mage", mage)
				.append("type", "memid").append("memid", ""));
		DBObject updateSetValue = new BasicDBObject("$set", updatedValue);
		dBCollection.update(updateCondition, updateSetValue); 
	}
	
	public void insertUserDetail(String memid,String msex,String mage) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todayStr = sdf.format(today);
		DBObject documents = new BasicDBObject("user_id",memid).append("create_date", todayStr).append("update_date", todayStr)
				.append("user_info", new BasicDBObject("msex", msex).append("mage", mage).append("memid", "").append("type", "memid"));
		dBCollection.insert(documents);
	}
	
	public Map<String, String> forecastPersonalInfo(String category) throws Exception {
		combinedValue combineObj = DmpLogMapper.clsfyCraspMap.get(category);
		String sex = (combineObj != null) ? combineObj.gender : "";
		String age = (combineObj != null) ? combineObj.age : "";

		Map<String, String> map = new HashMap<String, String>();
		map.put("msex", sex);
		map.put("mage", age);
		return map;
	}
	

	public Map<String, String> findMemberInfoAPI(String memid) throws Exception {
		url.setLength(0);
		url.append("http://member.pchome.com.tw/findMemberInfo4ADAPI.html?ad_user_id=");
		url.append(memid);
		prsnlData = httpGet(url.toString());
		String msex = JsonPath.parse(prsnlData).read("sexuality");
		String mage = JsonPath.parse(prsnlData).read("birthday");
		Map<String, String> memberInfoMapApi = new HashMap<String, String>();
		memberInfoMapApi.put("msex", StringUtils.isNotBlank(msex) ? msex : "NA");
		if ( StringUtils.isNotBlank(mage) ){
			memberInfoMapApi.put("mage", mage.split("-")[0]);
		}else{
			memberInfoMapApi.put("mage","NA");
		}
		return memberInfoMapApi;
	}
	
	
	public int getAge(Date birthDay) {
		Calendar cal = Calendar.getInstance();

		if (cal.before(birthDay)) {
			return -1;
		}

		int yearNow = cal.get(Calendar.YEAR);
		cal.setTime(birthDay);
		int yearBirth = cal.get(Calendar.YEAR);
		int age = yearNow - yearBirth;
		
		return age;
	}
	

	public String httpGet(String myURL) {
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		InputStreamReader in = null;
		try {
			URL url = new URL(myURL);
			urlConn = url.openConnection();
			if (urlConn != null)
				urlConn.setReadTimeout(60 * 1000);
			if (urlConn != null && urlConn.getInputStream() != null) {
				in = new InputStreamReader(urlConn.getInputStream(), "UTF-8");
				BufferedReader bufferedReader = new BufferedReader(in);
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			}
			in.close();
		} catch (Exception e) {
			log.error("findMemberInfoAPI Error : " + e.getMessage());
		}
		return sb.toString();
	}
}