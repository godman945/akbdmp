package com.pchome.hadoopdmp.enumerate;

public enum PersonalInfoEnum {

	MEMBER("MEMBER", "會員"),
	UUID("UUID", "UUID");
	
	private final String key;
	private final String value;

	private PersonalInfoEnum(String key, String value) {
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
