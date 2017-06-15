package com.pchome.hadoopdmp.factory.job;


import java.util.HashMap;
import java.util.Map;

import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase1Job;


public class FactoryPersonalData {
	private static Map<String, AncestorJob> register = new HashMap<String, AncestorJob>();

	public static AncestorJob getInstance(EnumPersonalDataPhase1Job enumPersonalDataPhase1Job) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		AncestorJob job = register.get(enumPersonalDataPhase1Job.toString());

		if( job==null ) {
			job = (AncestorJob) Class.forName(enumPersonalDataPhase1Job.getClassName()).newInstance();
			register.put(enumPersonalDataPhase1Job.toString(), job);
		}

		return job;
	}

}
