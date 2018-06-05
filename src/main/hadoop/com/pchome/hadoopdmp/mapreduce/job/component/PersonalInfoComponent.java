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

	// 處理個資元件
	public DmpLogBean processPersonalInfo(DmpLogBean dmpDataBean ,DB mongoOperations) throws Exception {
		
//		log.info(" >>>>>>>> processPersonalInfo mongoOperations to String : " +mongoOperations.getClass());	//test
		
//		long startAll, endAll;	//test
//		startAll = System.currentTimeMillis();//test
		
		
		String memid = dmpDataBean.getMemid();
		String category = dmpDataBean.getCategory();
		

//		//test
//		if ((StringUtils.isNotBlank(memid)) && (!memid.equals("null"))) {
//			long mongo1, mongo2;	//test
//			mongo1 = System.currentTimeMillis();	//test
//			
//			DBCollection dBCollection = mongoOperations.getCollection("user_detail");
//			BasicDBObject andQuery = new BasicDBObject();
//			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
//			obj.add(new BasicDBObject("user_id", memid));
//			andQuery.put("$and", obj);
//			DBObject dbObject =  dBCollection.findOne(andQuery);
//			
//			mongo2 = System.currentTimeMillis();	//test
//			
//			log.info(" >>>>>>>> org mongo userDetail : " +dbObject);	//test
//			log.info(" >>>>>>>> org query mongo userDetail cost " + (mongo2-mongo1) + " ms");	//test
//		}
//		//test
		
		
		
		
		// 如有memid資料，先查mongo，再撈會員中心查個資
		// 撈回mongo為NA也算已打過會員中心API，不再重打會員中心api
		if ((StringUtils.isNotBlank(memid)) && (!memid.equals("null"))) {
			
			long mongo1, mongo2;	//test
			mongo1 = System.currentTimeMillis();	//test
			
			DBCollection dBCollection = mongoOperations.getCollection("user_detail");
			BasicDBObject andQuery = new BasicDBObject();
			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
			obj.add(new BasicDBObject("user_id", memid));
			andQuery.put("$and", obj);
			DBObject dbObject =  dBCollection.findOne(andQuery);
			
			mongo2 = System.currentTimeMillis();	//test
			
//			log.info(" >>>>>>>> org mongo userDetail : " +dbObject);	//test
//			log.info(" >>>>>>>> org query mongo userDetail cost " + (mongo2-mongo1) + " ms");	//test
			
			
			
			
			String msex = "";
			String mage = "";
			if (dbObject != null) {
				// 查看user_detail結構中有無mage和msex
				String userInfoStr = dbObject.get("user_info").toString();
				 if ( (!userInfoStr.contains("mage")) || (!userInfoStr.contains("msex")) ){
					// Mongo沒有mage、msex資料空的打會員中心 API
					// 會員中心有資料寫回 mogodb msex mage 
					// 會員中心沒有資料寫入 NA
					
//					long member1, member2;	//test
//					member1 = System.currentTimeMillis();	//test
					
					Map<String, Object> memberInfoMap = findMemberInfoAPI(memid);
					
//					member2 = System.currentTimeMillis();	//test
//					log.info(" >>>>>>>>User exist - query MemberApi cost " + (member2-member1) + " ms");	//test
					
					
					msex = (String) memberInfoMap.get("msex");
					mage = (String) memberInfoMap.get("mage");
					
					
					
//					Update realPersonalData = new Update();
//					realPersonalData.set("user_info.type", "memid");
//					realPersonalData.set("user_info.memid", "");
//					realPersonalData.set("user_info.msex", msex);
//					realPersonalData.set("user_info.mage", mage);
//					mongoOperations.updateFirst(new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(memid)), realPersonalData,"user_detail");
//					
					//new
//					DBCollection authors = mongoOperations.getCollection("user_detail");
					DBObject updateCondition = new BasicDBObject();
					updateCondition.put("user_id", memid);
					DBObject updatedValue = new BasicDBObject();
					updatedValue.put("user_info", new BasicDBObject("msex", msex).append("mage", mage)
							.append("type", "memid").append("memid", ""));
					DBObject updateSetValue = new BasicDBObject("$set", updatedValue);
					dBCollection.update(updateCondition, updateSetValue); 
					//new
					
					
					
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
				
//				long member11, member22;	//test
//				member11 = System.currentTimeMillis();	//test
				
				Map<String, Object> memberInfoMap = findMemberInfoAPI(memid);
				
//				member22 = System.currentTimeMillis();	//test
//				log.info(" >>>>>>>>User Not Exist - query MemberApi cost " + (member22-member11) + " ms");	//test
				
				
				
				msex = (String) memberInfoMap.get("msex");
				mage = (String) memberInfoMap.get("mage");
				
//				//old
//				Map<String, String> map = new HashMap<String, String>();
//				map.put("type", "memid");
//				map.put("memid", "");
//				map.put("mage", mage);
//				map.put("msex", msex);
//				UserDetailMongoBeanForHadoop hadoopUserDetailBean = new UserDetailMongoBeanForHadoop();
//				hadoopUserDetailBean.setUser_id(memid);
//				hadoopUserDetailBean.setCreate_date(todayStr);
//				hadoopUserDetailBean.setUpdate_date(todayStr);
//				hadoopUserDetailBean.setUser_info(map);
//				mongoOperations.save(hadoopUserDetailBean);
//				//old
				
				//new
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date today = new Date();
				String todayStr = sdf.format(today);
				DBObject documents = new BasicDBObject("user_id",memid).append("create_date", todayStr).append("update_date", todayStr)
						.append("user_info", new BasicDBObject("msex", msex).append("mage", mage).append("memid", "").append("type", "memid"));
				dBCollection.insert(documents);
				//new
				
				
				
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
		
		// 讀取ClsfyGndAgeCrspTable.txt做age、sex個資推估
//		long forecast1, forecast2;	//test
//		forecast1 = System.currentTimeMillis();	//test
		
		Map<String, String> forecastInfoMap = forecastPersonalInfo(category);
		
//		forecast2 = System.currentTimeMillis();	//test
//		
//		log.info(" >>>>>>>>forecastPersonalInfo cost " + (forecast2-forecast1) + " ms");	//test
		
		
		String sex = forecastInfoMap.get("sex");
		String age = forecastInfoMap.get("age");
		
		dmpDataBean.setSex(sex);
		dmpDataBean.setSexSource( StringUtils.equals(sex, "null") ? "null" : "excel" ); 
		dmpDataBean.setAge(age);
		dmpDataBean.setAgeSource( StringUtils.equals(age, "null") ? "null" : "excel" );
		
		if ( (!StringUtils.equals(age, "null")) && (!StringUtils.equals(sex, "null")) ) {
			dmpDataBean.setPersonalInfoClassify("Y");
		} else {
			dmpDataBean.setPersonalInfoClassify("N");
		}
		
		
//		endAll = System.currentTimeMillis();	//test
		
//		log.info(" >>>>>>>> PersonalInfoComponent All cost " + (endAll-startAll) + " ms");	//test
		
		return dmpDataBean;
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