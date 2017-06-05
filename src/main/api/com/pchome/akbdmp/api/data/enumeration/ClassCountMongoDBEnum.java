package com.pchome.akbdmp.api.data.enumeration;

public enum ClassCountMongoDBEnum {
	TABLE_NAME("class_count","class_count"),
	ID("_id","id"),
	USER_ID("user_id","userId");
	
	private final String key;
	private final String jsonKey;
	private ClassCountMongoDBEnum(String key,String jsonKey) {
		this.key = key;
		this.jsonKey = jsonKey;
	 }
	public String getKey() {
		return key;
	}
	public String getJsonKey() {
		return jsonKey;
	}
	
	
}