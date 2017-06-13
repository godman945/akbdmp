package com.pchome.dmp.mapreduce.job.factory;

import java.util.Map;

import com.pchome.dmp.mapreduce.prsnldata.PersonalDataPhase2Mapper.combinedValue;

public class PersonalUuidInfo extends APersonalInfo {

	public Object personalData(Map<String, Object> map) throws Exception {
		String adClass = (String) map.get("ad_class");
		Map<String, combinedValue> clsfyCraspMap = (Map<String, combinedValue>) map.get("clsfyCraspMap");
		// 取得sex,age
		/*--- memid is Blank ---*/
		combinedValue combineObj = clsfyCraspMap.get(adClass);
		if (combineObj == null) {
			// log.info("combineObj is null, cause no crsp to clsfyCraspMap");
			return null;
		}

		String f =  combineObj.age;
		// combineObj.gender;
		return null;
	}

}