package com.pchome.dmp.mapreduce.job.factory;

public abstract class ACategoryLogData {
	private ACategoryLogData aCategoryLogData;
	
	public abstract Object processCategory(String[] values,CategoryLogBean categoryLogBean) throws Exception;

	public ACategoryLogData getACategoryLogData(Object obj) {
		return aCategoryLogData;
	}
}
