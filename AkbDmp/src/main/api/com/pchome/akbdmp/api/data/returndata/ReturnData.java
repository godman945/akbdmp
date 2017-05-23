package com.pchome.akbdmp.api.data.returndata;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("request")
public class ReturnData {

	private Object result;
	private boolean status;
	private String code;

	public ReturnData() {
		System.err.println("=============init ReturnData==========");
	}

	public ReturnData getReturnData(Object result, boolean status, String code) {
		this.result = result;
		this.status = status;
		this.code = code;
		return this;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}


}
