package com.pchome.dmp.mapreduce.job.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.pchome.dmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue;

public class CategoryLogBean {
	private String memid;
	private String uuid;
	private String adClass;
	private String sex;
	private String age;
	private String recodeDate;
	private Map<String, combinedValue> clsfyCraspMap ;
	private ArrayList<Map<String, String>> list; 
	
	
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

	

}
