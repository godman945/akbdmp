package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.mongodb.core.MongoOperations;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = values[1];
		String uuid = values[2];
		String adClass = values[15];
		String behaviorClassify = "Y";
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null")) ) {
			return null;
		}
		
		if (adClass.matches("\\d{16}")) {
			return null;
		}
		
		categoryLogBean.setAdClass(adClass);
		categoryLogBean.setMemid(values[1]);
		categoryLogBean.setUuid(values[2]);
		categoryLogBean.setSource("adclick");
		categoryLogBean.setBehaviorClassify(behaviorClassify);
		
		return categoryLogBean;
	}
}