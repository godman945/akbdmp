package com.pchome.hadoopdmp.mapreduce.job.component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.jayway.jsonpath.JsonPath;
import com.pchome.akbdmp.api.data.enumeration.ClassCountMongoDBEnum;
import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBeanForHadoop;
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper;
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogBean;

public class PersonalInfoComponent {
	
	Log log = LogFactory.getLog("PersonalInfoComponent");

	// 處理個資元件
	public CategoryLogBean processPersonalInfo(CategoryLogBean categoryLogBean ,MongoOperations mongoOperations) throws Exception {
		String memid = categoryLogBean.getMemid();
		String adClass = categoryLogBean.getAdClass();

		// 如有memid資料，先查mongo，再撈會員中心查個資
		// 撈回mongo為NA也算已打過會員中心API，不再重打會員中心api
		if ((StringUtils.isNotBlank(memid)) && (!memid.equals("null"))) {
			Query queryUserInfo = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(memid));
			UserDetailMongoBean userDetailMongoBean = mongoOperations.findOne(queryUserInfo, UserDetailMongoBean.class);
			String msex = "";
			String mage = "";
			if (userDetailMongoBean != null) {
				// 查看user_detail結構中有無mage和msex
				Map<String, Object> userInfoMap = new HashMap<String, Object>();
				userInfoMap = userDetailMongoBean.getUser_info();
				if ((userInfoMap.get("mage") == null) || (userInfoMap.get("msex") == null)) {
					// 沒有資料空的打會員中心 API
					// 會員中心有資料寫回 mogodb msex mage 
					// 會員中心沒有資料寫入 NA
					Map<String, Object> memberInfoMap = findMemberInfoAPI(memid);
					msex = (String) memberInfoMap.get("msex");
					mage = (String) memberInfoMap.get("mage");

					Update realPersonalData = new Update();
					realPersonalData.set("user_info.msex", msex);
					realPersonalData.set("user_info.mage", mage);
					mongoOperations.updateFirst(new Query(Criteria.where("user_id").is(memid)), realPersonalData,"user_detail");
					
					categoryLogBean.setMsex("null");
					categoryLogBean.setMage("null");
					
					if ( (!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA")) ) {
						categoryLogBean.setPersonalInfoMemberApiClassify("Y");
					} else {
						categoryLogBean.setPersonalInfoMemberApiClassify("N");
					}
				}else{
					// mongodb已有資料就跳過,包括NA (mongo user_detail結構中已有mage和msex)
					categoryLogBean.setMsex("null");
					categoryLogBean.setMage("null");
					categoryLogBean.setPersonalInfoMemberApiClassify("Y");
				}
				
			} else {
				// mongo尚未新增user_detail，直接新增一筆mongo資料，塞會員中心打回來的性別、年齡(空的轉成NA寫入)
				Map<String, Object> memberInfoMap = findMemberInfoAPI(memid);
				msex = (String) memberInfoMap.get("msex");
				mage = (String) memberInfoMap.get("mage");
				
				Map<String, String> map = new HashMap<String, String>();
				map.put("mage", mage);
				map.put("msex", msex);
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date today = new Date();
				String todayStr = sdf.format(today);

				UserDetailMongoBeanForHadoop hadoopUserDetailBean = new UserDetailMongoBeanForHadoop();
				hadoopUserDetailBean.setUser_info(map);
				hadoopUserDetailBean.setUser_id(memid);
				hadoopUserDetailBean.setCreate_date(todayStr);
				hadoopUserDetailBean.setUpdate_date(todayStr);
				
				mongoOperations.save(hadoopUserDetailBean);
				
				if ( (!StringUtils.equals(msex, "NA")) && (!StringUtils.equals(mage, "NA")) ) {
					categoryLogBean.setPersonalInfoMemberApiClassify("Y");
				} else {
					categoryLogBean.setPersonalInfoMemberApiClassify("N");
				}
			}
		}
		
		
		// 讀取ClsfyGndAgeCrspTable.txt做age、sex個資推估
		Map<String, String> forecastInfoMap = forecastPersonalInfo(adClass);
		String sex = StringUtils.isNotBlank(forecastInfoMap.get("sex")) ? forecastInfoMap.get("sex") : "null";
		String age = StringUtils.isNotBlank(forecastInfoMap.get("age")) ? forecastInfoMap.get("age") : "null";
	
		categoryLogBean.setSex(sex);
		categoryLogBean.setAge(age);
		
		if ( (!StringUtils.equals(age, "null")) && (!StringUtils.equals(sex, "null")) ) {
			categoryLogBean.setPersonalInfoClassify("Y");
		} else {
			categoryLogBean.setPersonalInfoClassify("N");
		}

		return categoryLogBean;
	}
	
	
	
	public Map<String, String> forecastPersonalInfo(String adClass) throws Exception {
		combinedValue combineObj = CategoryLogMapper.clsfyCraspMap.get(adClass);
		String sex = combineObj.gender;
		String age = combineObj.age;
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("sex", sex);
		map.put("age", age);
		return map;
	}
	

	public Map<String, Object> findMemberInfoAPI(String memid) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
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