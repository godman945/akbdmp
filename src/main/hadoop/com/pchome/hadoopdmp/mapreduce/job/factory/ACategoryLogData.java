package com.pchome.hadoopdmp.mapreduce.job.factory;

import com.mongodb.DBCollection;

public abstract class ACategoryLogData {
	private ACategoryLogData aCategoryLogData;
	
	public abstract Object processCategory(net.minidev.json.JSONObject dmpJSon, DBCollection dbCollection) throws Exception;

	public ACategoryLogData getACategoryLogData(Object obj) {
		return aCategoryLogData;
	}
}
