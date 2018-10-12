package com.pchome.hadoopdmp.mapreduce.job.factory;

import com.mongodb.DB;

public abstract class ACategoryLogData {
	private ACategoryLogData aCategoryLogData;
	
	public abstract Object processCategory(DmpLogBean dmpDataBean, DB mongoOperations) throws Exception;

	public ACategoryLogData getACategoryLogData(Object obj) {
		return aCategoryLogData;
	}
}
