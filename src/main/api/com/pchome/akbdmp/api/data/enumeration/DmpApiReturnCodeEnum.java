package com.pchome.akbdmp.api.data.enumeration;

public enum DmpApiReturnCodeEnum {
	
	API_CODE_S001(Boolean.TRUE, "呼叫成功","S001"),
	API_CODE_E001(Boolean.FALSE, "呼叫顯示頻次API,查詢序號不可為空","E001"),
	API_CODE_E002(Boolean.FALSE, "呼叫顯示頻次API,ERROR","E002"),
	API_CODE_E003(Boolean.FALSE, "Redis不可使用","E003");
	
	private final boolean status;
	private final String content;
	private final String code;
	private DmpApiReturnCodeEnum(boolean status, String content, String code) {
		this.status = status;
		this.content = content;
		this.code = code;
	 }
	public boolean isStatus() {
		return status;
	}
	public String getContent() {
		return content;
	}
	public String getCode() {
		return code;
	}
}

