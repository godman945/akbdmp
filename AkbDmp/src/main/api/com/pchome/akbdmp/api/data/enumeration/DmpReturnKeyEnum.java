package com.pchome.akbdmp.api.data.enumeration;

public enum DmpReturnKeyEnum {
	STATUS("status"), 
	RESULT("result"), 
	CODE("code");

	private final String key;

	private DmpReturnKeyEnum(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

}