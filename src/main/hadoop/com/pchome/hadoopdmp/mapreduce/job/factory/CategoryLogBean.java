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
	private String behaviorClassify;
	private String personalInfoClassify;
	private Map<String, combinedValue> clsfyCraspMap;
	private ArrayList<Map<String, String>> list;

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

	public String getBehaviorClassify() {
		return behaviorClassify;
	}

	public void setBehaviorClassify(String behaviorClassify) {
		this.behaviorClassify = behaviorClassify;
	}

	public String getPersonalInfoClassify() {
		return personalInfoClassify;
	}

	public void setPersonalInfoClassify(String personalInfoClassify) {
		this.personalInfoClassify = personalInfoClassify;
	}

}
