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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.enumerate.CategoryAgeEnum;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogReducer;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogReducer.combinedValue;

public class PersonalInfoComponent {
	
	Log log = LogFactory.getLog("PersonalInfoComponent");
	
	private static DBCollection dBCollection;
	private static String memid = "";
	private static String uuid = "";
	private static String category = "";
	private static DBObject dbObject;
	private static String userInfoStr;
	private static String msex = "";
	private static String mage = "";
	private static Map<String, Map<String, String>> sexAgeInfoMap = new HashMap<String, Map<String,String>>();
	private static StringBuffer url = new StringBuffer();
	private static String prsnlData = "";
	private static Calendar calendar = Calendar.getInstance();
	private static int count = 0;
	// 處理個資元件
	public net.minidev.json.JSONObject processPersonalInfo(net.minidev.json.JSONObject dmpJSon ,DBCollection dbCollectionUser) throws Exception {
		this.userInfoStr = "";
		this.dBCollection = dbCollectionUser;
		this.memid = dmpJSon.getAsString("memid");
		this.category = dmpJSon.getAsString("category");
		this.uuid = dmpJSon.getAsString("uuid");
		dbObject = null;
		// 如有memid資料，先查mongo，再撈會員中心查個資
		if(sexAgeInfoMap.containsKey(this.uuid+"<PCHOME>"+memid+"<PCHOME>"+category)) {
			Map<String, String> personalInfoMap = sexAgeInfoMap.get(this.uuid+"<PCHOME>"+memid+"<PCHOME>"+category);
			msex = (String) personalInfoMap.get("msex");
			mage = (String) personalInfoMap.get("mage");
			
			
			int age = 0;
			if(mage.equals("NA") || StringUtils.isBlank(mage)) {
				dmpJSon.put("age", "");
			}else {
				if(mage.length() == 4) {
					calendar.setTime(new Date());
					age = calendar.get(Calendar.YEAR) - Integer.parseInt(mage);
					dmpJSon.put("age", age);
				}else {
					dmpJSon.put("age", mage);
				}
			}
			if(msex.equals("NA") || StringUtils.isBlank(msex)) {
				dmpJSon.put("sex", "");
			}else {
				dmpJSon.put("sex", msex);
			}
			
			
			if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
				dmpJSon.put("personal_info_api_classify", "Y");
			} else {
				dmpJSon.put("personal_info_api_classify", "N");
			}
			if(StringUtils.isNotBlank(dmpJSon.getAsString("category"))) {
				dmpJSon.put("sex_source", StringUtils.equals(msex, "NA") ? "" : "excel");
				dmpJSon.put("age_source", StringUtils.equals(mage, "NA") ? "" : "excel");
			}
		}else {
			//memid資料是否存在時  1.有資料-->查詢mongodb是否已存-->未存則打會員API新增一筆資料，已存則取出 
			if (StringUtils.isNotBlank(memid)) {
//				log.info(">>>>>>memid:"+memid);
				msex = "";
				mage = "";
				Map<String, String> memberInfoMapApi = null;
				dbObject = queryUserDetail(memid);
				//mongo DB中有資料
				if (dbObject != null) {
					userInfoStr = dbObject.get("user_info").toString();
					// mongo DB中尚未打過會員中心取得年齡性別資訊
					if ((!userInfoStr.contains("mage")) || (!userInfoStr.contains("msex"))){
						//呼叫會員中心
						memberInfoMapApi = findMemberInfoAPI(memid);
						msex = (String) memberInfoMapApi.get("msex");
						mage = (String) memberInfoMapApi.get("mage");
						//更新user資料
						updateUserDetail(memid,msex,mage);
						int age = 0;
						if(!mage.equals("NA") && StringUtils.isNotBlank(mage)) {
							calendar.setTime(new Date());
							age = calendar.get(Calendar.YEAR) - Integer.parseInt(mage);
							dmpJSon.put("age", age);
						}else {
							dmpJSon.put("age", "");
						}
						if(!msex.equals("NA") && StringUtils.isNotBlank(msex)) {
							dmpJSon.put("sex", msex.toUpperCase());
						}else {
							dmpJSon.put("sex", "");
						}
						dmpJSon.put("sex_source","member_api");
						dmpJSon.put("age_source","member_api");
					}
					//已經打過會員中心資料
					if ((userInfoStr.contains("mage")) && (userInfoStr.contains("msex"))){
						msex = (String) ((DBObject)dbObject.get("user_info")).get("msex");
						mage = (String) ((DBObject)dbObject.get("user_info")).get("mage");
						int age = 0;
						if(!mage.equals("NA") && StringUtils.isNotBlank(mage)) {
							calendar.setTime(new Date());
							age = calendar.get(Calendar.YEAR) - Integer.parseInt(mage);
							dmpJSon.put("age", age);
						}else {
							dmpJSon.put("age", "");
						}
						if(!msex.equals("NA") && StringUtils.isNotBlank(msex)) {
							dmpJSon.put("sex", msex.toUpperCase());
						}else {
							dmpJSon.put("sex", "");
						}
						dmpJSon.put("sex_source","member_api");
						dmpJSon.put("age_source","member_api");
						
						memberInfoMapApi = new HashMap<String, String>();
						memberInfoMapApi.put("msex", msex);
						memberInfoMapApi.put("mage", mage);
					}
						
					if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
						dmpJSon.put("personal_info_api_classify", "Y");
					} else {
						dmpJSon.put("personal_info_api_classify", "N");
					}
					sexAgeInfoMap.put(dmpJSon.getAsString("uuid")+"<PCHOME>"+memid+"<PCHOME>"+category, memberInfoMapApi);
				}
				
				//mongo DB中無資料
				if (dbObject == null) {
//					log.info(">>>>>>2");
					// mongo尚未新增user_detail，直接新增一筆mongo資料，塞會員中心打回來的性別、年齡(空的轉成NA寫入)
					memberInfoMapApi = findMemberInfoAPI(memid);
					msex = (String) memberInfoMapApi.get("msex");
					mage = (String) memberInfoMapApi.get("mage");
					//新增user
					insertUserDetail(memid,msex,mage);
					int age = 0;
					if(mage.equals("NA") || StringUtils.isBlank(mage)) {
						dmpJSon.put("age", "");
					}else {
						if(mage.length() == 4) {
							calendar.setTime(new Date());
							age = calendar.get(Calendar.YEAR) - Integer.parseInt(mage);
							dmpJSon.put("age", age);
						}else {
							dmpJSon.put("age", mage);
						}
					}
					if(!msex.equals("NA") && StringUtils.isNotBlank(msex)) {
						dmpJSon.put("sex", msex.toUpperCase());
					}else {
						dmpJSon.put("sex", "");
					}
					dmpJSon.put("sex_source","member_api");
					dmpJSon.put("age_source","member_api");
					if ((!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA"))) {
						dmpJSon.put("personal_info_api_classify", "Y");
					} else {
						dmpJSon.put("personal_info_api_classify", "N");
					}
				}
				dmpJSon.put("sex_source","member_api");
				dmpJSon.put("age_source","member_api");
				
				sexAgeInfoMap.put(this.uuid+"<PCHOME>"+memid+"<PCHOME>"+category, memberInfoMapApi);
//				log.info(">>>>>>memberInfoMapApi:"+memberInfoMapApi);
				
			}else {
				if(StringUtils.isNotBlank(dmpJSon.getAsString("category"))) {
					//處理個資推估 
					Map<String, String> forecastPersonalInfoMap = forecastPersonalInfo(category);
					String msex = forecastPersonalInfoMap.get("msex");
					String mage = forecastPersonalInfoMap.get("mage");
					if(!mage.equals("NA") && StringUtils.isNotBlank(mage)) {
						dmpJSon.put("age", mage);
					}else {
						dmpJSon.put("age", "");
					}
					if(!msex.equals("NA") && StringUtils.isNotBlank(msex)) {
						dmpJSon.put("sex", msex.toUpperCase());
					}else {
						dmpJSon.put("sex", "");
					}
					dmpJSon.put("sex_source", StringUtils.equals(msex, "NA") ? "" : "excel");
					dmpJSon.put("age_source", StringUtils.equals(mage, "NA") ? "" : "excel");
					if ((!StringUtils.equals(mage, "NA")) && (!StringUtils.equals(msex, "NA"))) {
						dmpJSon.put("personal_info_classify", "Y");
					} else {
						dmpJSon.put("personal_info_classify", "N");
					}
					sexAgeInfoMap.put(this.uuid+"<PCHOME>"+memid+"<PCHOME>"+dmpJSon.getAsString("category"), forecastPersonalInfoMap);
				}
			}
		}
		
		if(StringUtils.isNotBlank(dmpJSon.getAsString("age"))) {
			int age = Integer.parseInt(dmpJSon.getAsString("age"));
			for (CategoryAgeEnum categoryAgeEnum : CategoryAgeEnum.values()) {
				if(age >= categoryAgeEnum.getMinimun() && age <= categoryAgeEnum.getMaximun()) {
					dmpJSon.put("age", categoryAgeEnum.getCode());
					break;
				}
			}
		}		
		if(this.uuid.equals("744d82cd-b1d3-4fd8-a7c1-1724901fe5f6")) {
			log.info(">>>>>>>>>>>end:"+dmpJSon);
		}
		return dmpJSon;
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
		updatedValue.put("user_info", new BasicDBObject("msex", msex).append("mage", mage).append("type", "memid").append("memid", ""));
		DBObject updateSetValue = new BasicDBObject("$set", updatedValue);
		dBCollection.update(updateCondition, updateSetValue); 
	}
	
	public void insertUserDetail(String memid,String msex,String mage) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todayStr = sdf.format(today);
		DBObject documents = new BasicDBObject("user_id",memid).append("create_date", todayStr).append("update_date", todayStr).append("user_info", new BasicDBObject("msex", msex).append("mage", mage).append("memid", "").append("type", "memid"));
		dBCollection.insert(documents);
	}
	// 讀取ClsfyGndAgeCrspTable.txt做age、sex個資推估
	public Map<String, String> forecastPersonalInfo(String category) throws Exception {
		combinedValue combineObj = DmpLogReducer.clsfyCraspMap.get(category);
		String sex = (combineObj != null) ? combineObj.gender : "NA";
		String age = (combineObj != null) ? combineObj.age : "NA";
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
		log.info(">>>>>>>>>>>prsnlData:"+prsnlData);
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