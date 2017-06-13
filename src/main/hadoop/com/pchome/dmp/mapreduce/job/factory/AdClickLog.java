package com.pchome.dmp.mapreduce.job.factory;

import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper.Context;

import com.pchome.dmp.enumerate.PersonalInfoEnum;

public class AdClickLog extends ACategoryLogData {

	public Object processCategory(String[] values, Object obj,CategoryLogBean categoryLogBean) throws Exception {
		PersonalInfoFactory personalInfoFactory = (PersonalInfoFactory) obj;
		APersonalInfo aPersonalInfo = personalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.MEMBER);
		Map<String,Object> map = aPersonalInfo.getMap();
		map.put("1", "memid");
		
		aPersonalInfo.personalData(map);
		
		
//		Map<String,Object> map = aPersonalInfo.getMap();
//		map.put("memid", values[1]);
//		aPersonalInfo.personalData(map);
		return null;
	}
}