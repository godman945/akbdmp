package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.ArrayList;
import java.util.Map;

import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper.combinedValue;

public class DmpLogBean {
	private Map<String, combinedValue> clsfyCraspMap;
	private ArrayList<Map<String, String>> list;
	
	//send kafka dada的key
	private String memid ;
	private String uuid ;

	//send kafka dada的data
	private String adClass ;
	private String adClassSource ;
	private String sex ;
	private String sexSource ;
	private String age ;
	private String ageSource ;
	private String areaInfoSource ;
	private String deviceInfoSource ;
	private String timeInfoSource ;
	private String recodeDate ;
	private String source ;
	private String type ;
	private String country ;
	private String city ;
	private String msex ;
	private String mage ;
	private String url ;
	private String ip ;
	private String userAgent ;
	private String dateTime ;
	private String dateTimeSource ;
	
	//send kafka dada的classify
	private String personalInfoApi ;
	private String personalInfo ;
	private String classAdClick ;
	private String class24hUrl ;
	private String classRutenUrl ;
	private String areaInfo ;
	private String deviceInfo ; 
	private String devicePhoneInfo ;
	private String deviceOsInfo ;
	private String deviceBrowserInfo;
	private String timeInfo ;
	private String deviceInfoClassify ;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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

	public String getRecodeDate() {
		return recodeDate;
	}

	public void setRecodeDate(String recodeDate) {
		this.recodeDate = recodeDate;
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

	public String getPersonalInfoApi() {
		return personalInfoApi;
	}

	public void setPersonalInfoApi(String personalInfoApi) {
		this.personalInfoApi = personalInfoApi;
	}

	public String getPersonalInfo() {
		return personalInfo;
	}

	public void setPersonalInfo(String personalInfo) {
		this.personalInfo = personalInfo;
	}

	public String getClassAdClick() {
		return classAdClick;
	}

	public void setClassAdClick(String classAdClick) {
		this.classAdClick = classAdClick;
	}

	public String getClass24hUrl() {
		return class24hUrl;
	}

	public void setClass24hUrl(String class24hUrl) {
		this.class24hUrl = class24hUrl;
	}

	public String getClassRutenUrl() {
		return classRutenUrl;
	}

	public void setClassRutenUrl(String classRutenUrl) {
		this.classRutenUrl = classRutenUrl;
	}

	public String getAreaInfo() {
		return areaInfo;
	}

	public void setAreaInfo(String areaInfo) {
		this.areaInfo = areaInfo;
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

	public String getTimeInfo() {
		return timeInfo;
	}

	public void setTimeInfo(String timeInfo) {
		this.timeInfo = timeInfo;
	}

	public String getAdClassSource() {
		return adClassSource;
	}

	public void setAdClassSource(String adClassSource) {
		this.adClassSource = adClassSource;
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

	public String getDateTimeSource() {
		return dateTimeSource;
	}

	public void setDateTimeSource(String dateTimeSource) {
		this.dateTimeSource = dateTimeSource;
	}
	
}
