package com.pchome.dmp.mapreduce.job.factory;

import java.util.HashMap;
import java.util.Map;

import com.pchome.dmp.enumerate.CategoryLogEnum;

public class CategoryLogFactory {

	private static Map<String, Object> objectMap = new HashMap<String, Object>();

	public ACategoryLogData getACategoryLogObj(CategoryLogEnum categoryLogEnum) throws Exception {

		switch (categoryLogEnum) {
		case AD_CLICK:
			if (objectMap.containsKey(categoryLogEnum.getKey())) {
				return (ACategoryLogData) objectMap.get(categoryLogEnum.getKey());
			} else {
				AdClickLog adClickLog = new AdClickLog();
				objectMap.put(categoryLogEnum.getKey(), adClickLog);
				return adClickLog;
			}
		case PV_RETUN:
			if (objectMap.containsKey(categoryLogEnum.getKey())) {
				return (ACategoryLogData) objectMap.get(categoryLogEnum.getKey());
			} else {
				AdClickLog adClickLog = new AdClickLog();
				objectMap.put(categoryLogEnum.getKey(), adClickLog);
				return adClickLog;
			}
		case PV_24H:
			if (objectMap.containsKey(categoryLogEnum.getKey())) {
				return (ACategoryLogData) objectMap.get(categoryLogEnum.getKey());
			} else {
				AdClickLog adClickLog = new AdClickLog();
				objectMap.put(categoryLogEnum.getKey(), adClickLog);
				return adClickLog;
			}
		default:
			break;
		}
		return null;
	}
}