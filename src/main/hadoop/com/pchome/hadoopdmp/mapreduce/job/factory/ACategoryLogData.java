package com.pchome.hadoopdmp.mapreduce.job.factory;

import org.springframework.data.mongodb.core.MongoOperations;

public abstract class ACategoryLogData {
	private ACategoryLogData aCategoryLogData;
	
	public abstract Object processCategory(String[] values,CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception;

	public ACategoryLogData getACategoryLogData(Object obj) {
		return aCategoryLogData;
	}
}
