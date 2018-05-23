package com.pchome.akbdmp.api.data.enumeration;

public enum DmpLogInfoKeyEnum {
	DEVICE_PHONE_INFO("device_phone_info","device_phone"),
	DEVICE_BROWSER_INFO("device_browser_info","browser"),
	DEVICE_OS_INFO("device_os_info","os"),
	DEVICE_INFO("device_info","device"),
	AREA_COUNTRY_INFO("area_country_info","country"),
	AGE_INFO("age_info","age"),
	AREA_CITY_INFO("area_city_info","city");

	private final String key;
	private final String mongoKey;

	private DmpLogInfoKeyEnum(String mongoKey,String key) {
		this.key = key;
		this.mongoKey = mongoKey;
	}

	public String getKey() {
		return key;
	}

	public String getMongoKey() {
		return mongoKey;
	}

}
