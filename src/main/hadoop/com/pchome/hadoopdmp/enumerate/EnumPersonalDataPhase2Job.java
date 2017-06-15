package com.pchome.hadoopdmp.enumerate;


public enum EnumPersonalDataPhase2Job {

//	Personal_Data_Ck(
//			"com.pchome.dmp.factory.job.PersonalDataCk"
//			),
	Personal_Data_Phase2_Pv(
			"com.pchome.dmp.factory.job.PersonalDataPhase2Pv"
			);

	private String className;

	private EnumPersonalDataPhase2Job(String className) {
		this.className = className;
	}

	public String getClassName() {
		return className;
	}



}
