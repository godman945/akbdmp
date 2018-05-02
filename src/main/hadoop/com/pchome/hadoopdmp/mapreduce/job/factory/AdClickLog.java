package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.mongodb.core.MongoOperations;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public Object processCategory(CategoryLogBean categoryRawDataBean, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = categoryRawDataBean.getMemid();
		String uuid = categoryRawDataBean.getUuid();
		String adClass = categoryRawDataBean.getAdClass();
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null")) ) {
			return null;
		}
		
		if (adClass.matches("\\d{16}")) {
			return null;
		}
		
		categoryLogBean.setAdClass(adClass);
		categoryLogBean.setMemid(memid);
		categoryLogBean.setUuid(uuid);
		categoryLogBean.setSource(categoryRawDataBean.getSource());
		categoryLogBean.setClassAdClick("Y");
		
		return categoryLogBean;
	}
}