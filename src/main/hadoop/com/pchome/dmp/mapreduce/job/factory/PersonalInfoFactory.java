package com.pchome.dmp.mapreduce.job.factory;

import java.util.HashMap;
import java.util.Map;

import com.pchome.dmp.enumerate.PersonalInfoEnum;

public class PersonalInfoFactory {

	private static Map<String, Object> objectMap = new HashMap<String, Object>();
	
	public APersonalInfo getAPersonalInfoFactory(PersonalInfoEnum personalInfoEnum) throws Exception {

		switch (personalInfoEnum) {
		case MEMBER:
			if(objectMap.containsKey(personalInfoEnum.getKey())){
				return (APersonalInfo) objectMap.get(personalInfoEnum.getKey());
			}else{
				PersonalMemberInfo memberInfo = new PersonalMemberInfo();
				objectMap.put(personalInfoEnum.getKey(), memberInfo);
				return memberInfo;
			}
		default:
			break;
		}
		return null;
	}
}