package com.pchome.hadoopdmp.factory.job;

import java.util.HashMap;
import java.util.Map;

import com.pchome.hadoopdmp.enumerate.EnumCategoryJob;


public class FactoryCategoryJob {
	private static Map<String, AncestorJob> register = new HashMap<String, AncestorJob>();

	public static AncestorJob getInstance(EnumCategoryJob enumCategoryJob) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		AncestorJob job = register.get(enumCategoryJob.toString());

		if( job==null ) {
			job = (AncestorJob) Class.forName(enumCategoryJob.getClassName()).newInstance();
			register.put(enumCategoryJob.toString(), job);
		}

		return job;
	}

}
