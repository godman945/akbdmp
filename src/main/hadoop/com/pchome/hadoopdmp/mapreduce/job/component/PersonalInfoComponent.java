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
	
	private DBCollection dBCollection;
	
	// 處理個資元件
	public DmpLogBean processPersonalInfo(DmpLogBean dmpDataBean ,DB mongoOperations) throws Exception {
		this.dBCollection= mongoOperations.getCollection("user_detail");
		
		String memid = dmpDataBean.getMemid();
		String category = dmpDataBean.getCategory();
		
		// 如有memid資料，先查mongo，再撈會員中心查個資
		// 撈回mongo為NA也算已打過會員中心API，不再重打會員中心api
		if ((StringUtils.isNotBlank(memid)) && (!memid.equals("null"))) {
			DBObject dbObject = queryUserDetail(memid);
			
			String msex = "";
			String mage = "";
			if (dbObject != null) {
				String userInfoStr = dbObject.get("user_info").toString();
				
				// mongo user_detail舊資料中有無mage、msex
				 if ( (!userInfoStr.contains("mage")) || (!userInfoStr.contains("msex")) ){
					Map<String, Object> memberInfoMapApi = findMemberInfoAPI(memid);
					msex = (String) memberInfoMapApi.get("msex");
					mage = (String) memberInfoMapApi.get("mage");
					//更新user資料
					updateUserDetail(memid,msex,mage);
					dmpDataBean.setMsex("null");
					dmpDataBean.setMage("null");
					if ( (!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA")) ) {
						dmpDataBean.setPersonalInfoApiClassify("Y");
					} else {
						dmpDataBean.setPersonalInfoApiClassify("N");
					}
				}else{
					// mongodb已有資料就跳過,包括NA (mongo user_detail結構中已有mage和msex)
					dmpDataBean.setMsex("null");
					dmpDataBean.setMage("null");
					dmpDataBean.setPersonalInfoApiClassify("Y");
				}
			} else {
				// mongo尚未新增user_detail，直接新增一筆mongo資料，塞會員中心打回來的性別、年齡(空的轉成NA寫入)
				Map<String, Object> memberInfoMapApi = findMemberInfoAPI(memid);
				msex = (String) memberInfoMapApi.get("msex");
				mage = (String) memberInfoMapApi.get("mage");
				//新增user
				insertUserDetail(memid,msex,mage);
				dmpDataBean.setMsex("null");
				dmpDataBean.setMage("null");
				if ( (!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA")) ) {
					dmpDataBean.setPersonalInfoApiClassify("Y");
				} else {
					dmpDataBean.setPersonalInfoApiClassify("N");
				}
			}
		}
		
		//如果raw data就有推估的age、sex，即PersonalInfo已被分類
		if ( (!StringUtils.equals(dmpDataBean.getSex(), "null")) && (!StringUtils.equals(dmpDataBean.getAge(), "null")) ){
			dmpDataBean.setPersonalInfoClassify("Y");
			dmpDataBean.setSexSource(dmpDataBean.getSource());
			dmpDataBean.setAgeSource(dmpDataBean.getSource());
			return dmpDataBean;
		}
		//處理個資推估
		dmpDataBean = processForecastPersonalInfo(dmpDataBean,category);
		
		return dmpDataBean;
	}
	

	
	public DmpLogBean processForecastPersonalInfo(DmpLogBean dmpDataBean, String category) throws Exception {
		// 讀取ClsfyGndAgeCrspTable.txt做age、sex個資推估
		Map<String, String> forecastInfoMap = forecastPersonalInfo(category);
		String sex = forecastInfoMap.get("sex");
		String age = forecastInfoMap.get("age");

		dmpDataBean.setSex(sex);
		dmpDataBean.setSexSource(StringUtils.equals(sex, "null") ? "null" : "excel");
		dmpDataBean.setAge(age);
		dmpDataBean.setAgeSource(StringUtils.equals(age, "null") ? "null" : "excel");

		if ((!StringUtils.equals(age, "null")) && (!StringUtils.equals(sex, "null"))) {
			dmpDataBean.setPersonalInfoClassify("Y");
		} else {
			dmpDataBean.setPersonalInfoClassify("N");
		}
		return dmpDataBean;
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
		String sex = (combineObj != null) ? combineObj.gender : "null";
		String age = (combineObj != null) ? combineObj.age : "null";

		Map<String, String> map = new HashMap<String, String>();
		map.put("sex", sex);
		map.put("age", age);
		return map;
	}
	

	public Map<String, Object> findMemberInfoAPI(String memid) throws Exception {
		StringBuffer url = new StringBuffer();
		url.append("http://member.pchome.com.tw/findMemberInfo4ADAPI.html?ad_user_id=");
		url.append(memid);
		String prsnlData = httpGet(url.toString());

		String msex = JsonPath.parse(prsnlData).read("sexuality");
		String mage = JsonPath.parse(prsnlData).read("birthday");

		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put("msex", StringUtils.isNotBlank(msex) ? msex : "NA");
		
		if ( StringUtils.isNotBlank(mage) ){
			map.put("mage", mage.split("-")[0]);
		}else{
			map.put("mage","NA");
		}

		return map;
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