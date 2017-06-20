package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.mongodb.core.MongoOperations;

import com.pchome.hadoopdmp.enumerate.PersonalInfoEnum;
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = values[1];
		String uuid = values[2];
		String adClass = values[15];
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null")) && adClass.matches("\\d{16}")) {
			return null;
		}
		
	    //取個資
	    if((StringUtils.isNotBlank(memid)) && (!memid.equals("null")) ) {
	    	categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ad_click");
			categoryLogBean.setType("memid");
			return categoryLogBean;
		}else if((StringUtils.isNotBlank(uuid)) && (!uuid.equals("null"))){
			APersonalInfo aPersonalInfo = PersonalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.UUID);
			Map<String, Object> uuidMap = aPersonalInfo.getMap();
			uuidMap.put("adClass", adClass); 
			uuidMap.put("ClsfyCraspMap", CategoryLogMapper.clsfyCraspMap);
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