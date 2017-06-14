package com.pchome.dmp.mapreduce.job.factory;

import java.util.HashMap;
import java.util.Map;

import com.pchome.dmp.enumerate.CategoryLogEnum;

public class CategoryLogFactory {

	private static Map<String, Object> objectMap = new HashMap<String, Object>();

	public static ACategoryLogData getACategoryLogObj(CategoryLogEnum categoryLogEnum) throws Exception {

		switch (categoryLogEnum) {
		case AD_CLICK:
			if (objectMap.containsKey(categoryLogEnum.getKey())) {
				return (AdClickLog) objectMap.get(categoryLogEnum.getKey());
			} else {
				AdClickLog adClickLog = new AdClickLog();
				objectMap.put(categoryLogEnum.getKey(), adClickLog);
				return adClickLog;
			}
		case PV_RETUN:
			if (objectMap.containsKey(categoryLogEnum.getKey())) {
				return (AdRutenLog) objectMap.get(categoryLogEnum.getKey());
			} else {
				AdRutenLog adRetunLog = new AdRutenLog();
				objectMap.put(categoryLogEnum.getKey(), adRetunLog);
				return adRetunLog;
			}
		case PV_24H:
			if (objectMap.containsKey(categoryLogEnum.getKey())) {
				return (Ad24HLog) objectMap.get(categoryLogEnum.getKey());
			} else {
				Ad24HLog ad24HLog = new Ad24HLog();
				objectMap.put(categoryLogEnum.getKey(), ad24HLog);
				return ad24HLog;
			}
		default:
			break;
		}
		return null;
	}
}