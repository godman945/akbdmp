package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.ArrayList;
import java.util.Map;

import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue;

public class CategoryLogBean {
	private String memid;
	private String uuid;
	private String adClass;
	private String sex;
	private String age;
	private String recodeDate;
	private String source;
	private String type;
//	private String behaviorClassify;
	private Map<String, combinedValue> clsfyCraspMap;
	private ArrayList<Map<String, String>> list;
	private String msex;
	private String mage;
//	private String personalInfoMemberApiClassify;
//	private String personalInfoClassify;
	private String url;
	private String personalInfoApi;
	private String personalInfo;
	private String classAdClick;
	private String class24hUrl;
	private String classRutenUrl;
	private String areaInfo;
	private String deviceInfo;
	private String devicePhoneInfo;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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
	
}
