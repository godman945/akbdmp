package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.MongoOperations;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public Object processCategory(DmpLogBean dmpDataBean, MongoOperations mongoOperations) throws Exception {
		
		String adClass = dmpDataBean.getAdClass();
		
		if (!adClass.matches("\\d{16}")) {
			dmpDataBean.setCategory("null");
			dmpDataBean.setCategorySource("null");
			dmpDataBean.setClassAdClickClassify("null");
		}
		
		
		dmpDataBean.setCategory(adClass);
		dmpDataBean.setClassAdClickClassify("Y");
		
		if ( StringUtils.equals(dmpDataBean.getSource(),"ck") ){
			dmpDataBean.setCategorySource("adclick");
			dmpDataBean.setSource("kdcl");
		}
		
		if ( StringUtils.equals(dmpDataBean.getSource(),"campaign") ){
			dmpDataBean.setCategorySource("campaign");
			dmpDataBean.setSource("campaign");
		}
		
		return dmpDataBean;
	}
}