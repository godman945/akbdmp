package com.pchome.dmp.enumerate;

public enum EnumCategoryJob {

	Category_Count_Ck(
			"com.pchome.dmp.factory.job.CategoryCountCk"
			),
	Category_Count_Pv(
			"com.pchome.dmp.factory.job.CategoryCountPv"
			);

	private String className;

	private EnumCategoryJob(String className) {
		this.className = className;
	}

	public String getClassName() {
		return className;
	}



}
