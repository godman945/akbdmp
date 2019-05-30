package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.DB;
import com.mongodb.DBCollection;

@SuppressWarnings({ "unchecked", "deprecation" ,"static-access","resource"})
public class AdClickLog extends ACategoryLogData {

	public net.minidev.json.JSONObject processCategory(net.minidev.json.JSONObject dmpJSon, DBCollection dbCollection) throws Exception {
		if (!dmpJSon.getAsString("ad_class").matches("\\d{16}")) {
			dmpJSon.put("class_adclick_classify", "N");
			return dmpJSon;
		}else {
			dmpJSon.put("category", dmpJSon.getAsString("ad_class"));
			dmpJSon.put("class_adclick_classify", "Y");
			if (StringUtils.equals(dmpJSon.getAsString("trigger_type"),"campaign") ){
				dmpJSon.put("category_source", "campaign");
			}
		}
		return dmpJSon;
		
//		String adClass = dmpDataBean.getAdClass();
//		
//		if (!adClass.matches("\\d{16}")) {
//			dmpDataBean.setCategory("null");
//			dmpDataBean.setCategorySource("null");
//			dmpDataBean.setClassAdClickClassify("N");
//			return dmpDataBean;
//		}
//		
//		
//		dmpDataBean.setCategory(adClass);
//		dmpDataBean.setClassAdClickClassify("Y");
//		
//		if ( StringUtils.equals(dmpDataBean.getSource(),"ck") ){
//			dmpDataBean.setCategorySource("adclick");
//			dmpDataBean.setSource("kdcl");
//		}
//		
//		if ( StringUtils.equals(dmpDataBean.getSource(),"campaign") ){
//			dmpDataBean.setCategorySource("campaign");
//			dmpDataBean.setSource("campaign");
//		}
//		
//		return dmpDataBean;
	}
}