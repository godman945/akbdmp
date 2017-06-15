package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.Map;

public class PersonalUuidInfo extends APersonalInfo {

	public Object personalData(Map<String, Object> map) throws Exception {
		String adClass = (String) map.get("adClass");
		
		Map<String, com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue> clsfyCraspMap = (Map<String, com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue>) map.get("ClsfyCraspMap");
		com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue combineObj = clsfyCraspMap.get(adClass);
		String sex = combineObj.gender;
		String age = combineObj.age;
		map.put("sex", sex);
		map.put("age", age);
		return map;
	}

}