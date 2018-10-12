package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.ArrayList;
import java.util.Map;

import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper.combinedValue;

public class DmpLogBean {
	//raw data 
	private String memid ;
	private String uuid ;
	private String url ;
	private String ip ;
	private String userAgent ;
	private String adClass ;
	private String source ;		//ck,pv,campaign
	private String dateTime ;
	private Map<String, combinedValue> clsfyCraspMap;
	private ArrayList<Map<String, String>> list;
	
	//地區處理元件(ip 轉國家、城市)
	private String country ;
	private String city ;
	private String areaInfoSource ;
	
	//時間處理元件(日期時間字串轉成小時)
	private String hour;
	private String timeInfoSource ;
	
	//裝置處理元件
	private String deviceInfo ; 
	private String devicePhoneInfo ;
	private String deviceOsInfo ;
	private String deviceBrowserInfo;
	private String deviceInfoSource ;
	
	//分類處理元件
	private String category ;
	private String categorySource ;			//adclick,24h,ruten,campaign
	
	//個資處理元件
	private String msex ;		//會員中心真實sex
	private String mage ;		//會員中心真實age
	private String sex ;		//推估性別
	private String sexSource ;	//推估性別來源
	private String age ;		//推估年齡
	private String ageSource ;	//推估年齡來源
	
	//紀錄日期
	private String recordDate ;
	
	//有無Classify
	private String areaInfoClassify = "null";
	private String timeInfoClassify = "null";
	private String deviceInfoClassify = "null";
	private String classAdClickClassify = "null";
	private String class24hUrlClassify = "null";
	private String classRutenUrlClassify = "null";
	private String personalInfoApiClassify = "null";
	private String personalInfoClassify = "null";
	
	//第3分類
	private ArrayList<String> prodClassInfo = new ArrayList<String>();
	private String urlToMd5 ="null";
	
	
	public String getUrlToMd5() {
		return urlToMd5;
	}

	public void setUrlToMd5(String urlToMd5) {
		this.urlToMd5 = urlToMd5;
	}

	public ArrayList<String> getProdClassInfo() {
		return prodClassInfo;
	}

	public void setProdClassInfo(ArrayList<String> prodClassInfo) {
		this.prodClassInfo = prodClassInfo;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getMemid() {
		return memid;
	}

	public void setMemid(String memid) {
		this.memid = memid;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getAdClass() {
		return adClass;
	}

	public void setAdClass(String adClass) {
		this.adClass = adClass;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getRecordDate() {
		return recordDate;
	}

	public void setRecordDate(String recordDate) {
		this.recordDate = recordDate;
	}

	public Map<String, combinedValue> getClsfyCraspMap() {
		return clsfyCraspMap;
	}

	public void setClsfyCraspMap(Map<String, combinedValue> clsfyCraspMap) {
		this.clsfyCraspMap = clsfyCraspMap;
	}

	public ArrayList<Map<String, String>> getList() {
		return list;
	}

	public void setList(ArrayList<Map<String, String>> list) {
		this.list = list;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getMsex() {
		return msex;
	}

	public void setMsex(String msex) {
		this.msex = msex;
	}

	public String getMage() {
		return mage;
	}

	public void setMage(String mage) {
		this.mage = mage;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
	
	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getPersonalInfoApiClassify() {
		return personalInfoApiClassify;
	}

	public void setPersonalInfoApiClassify(String personalInfoApiClassify) {
		this.personalInfoApiClassify = personalInfoApiClassify;
	}

	public String getPersonalInfoClassify() {
		return personalInfoClassify;
	}

	public void setPersonalInfoClassify(String personalInfoClassify) {
		this.personalInfoClassify = personalInfoClassify;
	}

	public String getDeviceInfo() {
		return deviceInfo;
	}

	public void setDeviceInfo(String deviceInfo) {
		this.deviceInfo = deviceInfo;
	}

	public String getDevicePhoneInfo() {
		return devicePhoneInfo;
	}

	public void setDevicePhoneInfo(String devicePhoneInfo) {
		this.devicePhoneInfo = devicePhoneInfo;
	}

	public String getDeviceOsInfo() {
		return deviceOsInfo;
	}

	public void setDeviceOsInfo(String deviceOsInfo) {
		this.deviceOsInfo = deviceOsInfo;
	}

	public String getDeviceBrowserInfo() {
		return deviceBrowserInfo;
	}

	public void setDeviceBrowserInfo(String deviceBrowserInfo) {
		this.deviceBrowserInfo = deviceBrowserInfo;
	}

	public String getSexSource() {
		return sexSource;
	}

	public void setSexSource(String sexSource) {
		this.sexSource = sexSource;
	}

	public String getAgeSource() {
		return ageSource;
	}

	public void setAgeSource(String ageSource) {
		this.ageSource = ageSource;
	}

	public String getAreaInfoSource() {
		return areaInfoSource;
	}

	public void setAreaInfoSource(String areaInfoSource) {
		this.areaInfoSource = areaInfoSource;
	}

	public String getDeviceInfoSource() {
		return deviceInfoSource;
	}

	public void setDeviceInfoSource(String deviceInfoSource) {
		this.deviceInfoSource = deviceInfoSource;
	}

	public String getTimeInfoSource() {
		return timeInfoSource;
	}

	public void setTimeInfoSource(String timeInfoSource) {
		this.timeInfoSource = timeInfoSource;
	}

	public String getDeviceInfoClassify() {
		return deviceInfoClassify;
	}

	public void setDeviceInfoClassify(String deviceInfoClassify) {
		this.deviceInfoClassify = deviceInfoClassify;
	}

	public String getAreaInfoClassify() {
		return areaInfoClassify;
	}

	public void setAreaInfoClassify(String areaInfoClassify) {
		this.areaInfoClassify = areaInfoClassify;
	}

	public String getTimeInfoClassify() {
		return timeInfoClassify;
	}

	public void setTimeInfoClassify(String timeInfoClassify) {
		this.timeInfoClassify = timeInfoClassify;
	}

	public String getClassAdClickClassify() {
		return classAdClickClassify;
	}

	public void setClassAdClickClassify(String classAdClickClassify) {
		this.classAdClickClassify = classAdClickClassify;
	}

	public String getClass24hUrlClassify() {
		return class24hUrlClassify;
	}

	public void setClass24hUrlClassify(String class24hUrlClassify) {
		this.class24hUrlClassify = class24hUrlClassify;
	}

	public String getClassRutenUrlClassify() {
		return classRutenUrlClassify;
	}

	public void setClassRutenUrlClassify(String classRutenUrlClassify) {
		this.classRutenUrlClassify = classRutenUrlClassify;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getCategorySource() {
		return categorySource;
	}

	public void setCategorySource(String categorySource) {
		this.categorySource = categorySource;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}
	
}
