//package com.pchome.hadoopdmp.mapreduce.job.factory;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import com.pchome.hadoopdmp.enumerate.PersonalInfoEnum;
//
//public class PersonalInfoFactory {
//
//	private static Map<String, Object> objectMap = new HashMap<String, Object>();
//	
//	public static APersonalInfo getAPersonalInfoFactory(PersonalInfoEnum personalInfoEnum) throws Exception {
//
//		switch (personalInfoEnum) {
//		case MEMBER:
//			if(objectMap.containsKey(personalInfoEnum.getKey())){
//				return (PersonalMemberInfo) objectMap.get(personalInfoEnum.getKey());
//			}else{
//				PersonalMemberInfo personalMemberInfo = new PersonalMemberInfo();
//				objectMap.put(personalInfoEnum.getKey(), personalMemberInfo);
//				return personalMemberInfo;
//			}
//		case UUID:
//			if(objectMap.containsKey(personalInfoEnum.getKey())){
//				return (PersonalUuidInfo) objectMap.get(personalInfoEnum.getKey());
//			}else{
//				PersonalUuidInfo personalUuidInfo = new PersonalUuidInfo();
//				objectMap.put(personalInfoEnum.getKey(), personalUuidInfo);
//				return personalUuidInfo;
//			}
//		default:
//			break;
//		}
//		return null;
//	}
//}