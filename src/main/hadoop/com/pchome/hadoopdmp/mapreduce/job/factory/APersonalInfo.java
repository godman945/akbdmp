package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class APersonalInfo {
	private APersonalInfo aPersonalInfo;

	private static Map<String, Object> map = new HashMap<>();

	public abstract Object personalData(Map<String, Object> map) throws Exception;
	
	public APersonalInfo getaPersonalInfo(Object obj) {
		return aPersonalInfo;
	}
	
	public static Map<String, Object> getMap() {
		return map;
	}

	public static void setMap(Map<String, Object> map) {
		APersonalInfo.map = map;
	}


	

}
