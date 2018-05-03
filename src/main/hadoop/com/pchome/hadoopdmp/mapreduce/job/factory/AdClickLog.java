package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.springframework.data.mongodb.core.MongoOperations;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public Object processCategory(CategoryLogBean dmpDataBean, MongoOperations mongoOperations) throws Exception {
		
		String adClass = dmpDataBean.getAdClass();
		
		if (!adClass.matches("\\d{16}")) {
			dmpDataBean.setClassAdClick("N");
		}
		
		dmpDataBean.setAdClass(adClass);
		dmpDataBean.setClassAdClick("Y");
//		dmpDataBean.setMemid(memid);
//		dmpDataBean.setUuid(uuid);
//		dmpDataBean.setSource(dmpDataBean.getSource());
		
		return dmpDataBean;
	}
}