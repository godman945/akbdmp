package com.pchome.dmp.mapreduce.job.factory;

import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class ACategoryLogData {
	private ACategoryLogData aCategoryLogData;
	
	public abstract Object processCategory(String[] values,Object obj,CategoryLogBean categoryLogBean) throws Exception;

	public ACategoryLogData getACategoryLogData(Object obj) {
		return aCategoryLogData;
	}
}
