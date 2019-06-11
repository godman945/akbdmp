package com.pchome.hadoopdmp.enumerate;

public enum CategoryAgeEnum {

	AGE_CODE_A("a", 0,17),
	AGE_CODE_B("b", 18,24),
	AGE_CODE_C("c", 25,34),
	AGE_CODE_D("d", 35,44),
	AGE_CODE_E("e", 45,54),
	AGE_CODE_F("f", 55,64),
	AGE_CODE_G("g",65,74),
	AGE_CODE_H("h", 75,999);
	
	private final String code;
	private final int minimun;
	private final int maximun;

	private CategoryAgeEnum(String code, int minimun,int maximun) {
		this.code = code;
		this.minimun = minimun;
		this.maximun = maximun;
	}


	public String getCode() {
		return code;
	}


	public int getMinimun() {
		return minimun;
	}

	public int getMaximun() {
		return maximun;
	}

}
