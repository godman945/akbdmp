package com.pchome.hadoopdmp.mapreduce.job.dmplog;

public enum AkbDmpLogInfoEnum {
	SEX_INFO("sex_info","sex"),
	DEVICE_PHONE_INFO("device_phone_info","device_phone"),
	DEVICE_BROWSER_INFO("device_browser_info","browser"),
	CATEGORY_INFO("category_info","category"),
	DEVICE_OS_INFO("device_os_info","os"),
	AGE_INFO("age_info","age"),
	TIME_INFO("time_info","time"),
	AREA_COUNTRY_INFO("area_country_info","country"),
	DEVICE_INFO("device_info","device"),
	AREA_CITY_INFO("area_city_info","city");

	private final String key;
	private final String infoKey;

	private AkbDmpLogInfoEnum(String key,String infoKey) {
		this.key = key;
		this.infoKey = infoKey;
	}

	public String getKey() {
		return key;
	}

	public String getInfoKey() {
		return infoKey;
	}

}
