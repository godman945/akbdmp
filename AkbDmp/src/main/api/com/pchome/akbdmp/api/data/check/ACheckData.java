package com.pchome.akbdmp.api.data.check;

import org.json.JSONObject;

public abstract class ACheckData {
	private ACheckData aCheckData;
	
	public abstract Object checkData(JSONObject json) throws Exception;

	public ACheckData getaCheckData(Object obj) {
		return aCheckData;
	}
}
