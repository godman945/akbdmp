package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.springframework.data.mongodb.core.MongoOperations;

import com.pchome.hadoopdmp.enumerate.PersonalInfoEnum;

public class AdClickLog extends ACategoryLogData {

	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = values[1];
		String uuid = values[2];
//		String sourceUrl = values[4];
		String adClass = values[15];
		
		
	    //取個資
	    if((StringUtils.isNotBlank(memid)) && (!memid.equals("null")) ) {
	    	categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ad_click");
			categoryLogBean.setType("memid");
			return categoryLogBean;
		}else if(StringUtils.isNotBlank(uuid)){
			APersonalInfo aPersonalInfo = PersonalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.UUID);
			Map<String, Object> uuidMap = aPersonalInfo.getMap();
			uuidMap.put("adClass", adClass); 
			uuidMap.put("ClsfyCraspMap", categoryLogBean.getClsfyCraspMap());
			Map<String, Object> userInfo = (Map<String, Object>) aPersonalInfo.personalData(uuidMap);
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ad_click");
			categoryLogBean.setSex(StringUtils.isNotBlank(userInfo.get("sex").toString()) ? userInfo.get("sex").toString(): "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(userInfo.get("age").toString()) ? userInfo.get("age").toString(): "null");
			categoryLogBean.setType("uuid");
			return categoryLogBean;
		}
		
		return null;
	}
}