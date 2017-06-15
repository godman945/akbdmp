package com.pchome.hadoopdmp.enumerate;


public enum EnumPersonalDataPhase1Job {

//	Personal_Data_Phase1_Ck(
//			"com.pchome.dmp.factory.job.PersonalDataPhase1Ck"
//			),
	Personal_Data_Phase1_Pv(
			"com.pchome.dmp.factory.job.PersonalDataPhase1Pv"
			);

	private String className;

	private EnumPersonalDataPhase1Job(String className) {
		this.className = className;
	}

	public String getClassName() {
		return className;
	}



}
