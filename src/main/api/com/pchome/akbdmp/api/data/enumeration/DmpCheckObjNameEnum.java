package com.pchome.akbdmp.api.data.enumeration;

public enum DmpCheckObjNameEnum {

	CHECK_ADSHOW_LIMIT("CHECK_ADSHOW_LIMIT", "檢查廣告頻次");
	
	private final String key;
	private final String value;

	private DmpCheckObjNameEnum(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

}
