package com.pchome.dmp.factory.job;


import java.util.HashMap;
import java.util.Map;

import com.pchome.dmp.enumerate.EnumPersonalDataPhase2Job;


public class FactoryPersonalDataPhase2 {
	private static Map<String, AncestorJob> register = new HashMap<String, AncestorJob>();

	public static AncestorJob getInstance(EnumPersonalDataPhase2Job enumPersonalDataPhase2Job) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		AncestorJob job = register.get(enumPersonalDataPhase2Job.toString());

		if( job==null ) {
			job = (AncestorJob) Class.forName(enumPersonalDataPhase2Job.getClassName()).newInstance();
			register.put(enumPersonalDataPhase2Job.toString(), job);
		}

		return job;
	}

}
