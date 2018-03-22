package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.pchome.akbdmp.api.data.enumeration.ClassCountMongoDBEnum;
import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.enumerate.PersonalInfoEnum;
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = values[1];
		String uuid = values[2];
		String adClass = values[15];
		String behaviorClassify = "Y";
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null")) && adClass.matches("\\d{16}")) {
			return null;
		}
		
	    //取個資
	    if((StringUtils.isNotBlank(memid)) && (!memid.equals("null")) ) {
	    	Query queryUserInfo = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
			UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(queryUserInfo, UserDetailMongoBean.class);
			String sex = "";
			String age = "";
			if(userDetailMongoBean != null){
				sex = (String)userDetailMongoBean.getUser_info().get("sex");
				age = (String)userDetailMongoBean.getUser_info().get("age");
				categoryLogBean.setPersonalInfoClassify("Y");
			}else{
				categoryLogBean.setPersonalInfoClassify("N");
			}
			categoryLogBean.setSex(StringUtils.isNotBlank(sex) ? sex : "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(age) ? age : "null");
	    	categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("adclick");
			categoryLogBean.setType("memid");
			categoryLogBean.setBehaviorClassify(behaviorClassify);
			
			return categoryLogBean;
		}else if((StringUtils.isNotBlank(uuid)) && (!uuid.equals("null"))){
			Query queryUserInfo = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
			UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(queryUserInfo, UserDetailMongoBean.class);
			String sex = "";
			String age = "";
			if(userDetailMongoBean != null){
				sex = (String)userDetailMongoBean.getUser_info().get("sex");
				age = (String)userDetailMongoBean.getUser_info().get("age");
				categoryLogBean.setPersonalInfoClassify("Y");
			}else{
				categoryLogBean.setPersonalInfoClassify("N");
			}
			categoryLogBean.setSex(sex);
			categoryLogBean.setAge(age);
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("adclick");
			categoryLogBean.setSex(StringUtils.isNotBlank(sex) ? sex : "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(age) ? age : "null");
			categoryLogBean.setType("uuid");
			categoryLogBean.setBehaviorClassify(behaviorClassify);
			return categoryLogBean;
		}
		
		return null;
	}
}