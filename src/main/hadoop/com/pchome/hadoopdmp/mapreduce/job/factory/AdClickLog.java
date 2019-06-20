package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.DBCollection;

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
	}
}