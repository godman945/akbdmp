package com.pchome.akbdmp.api.call.adshowlimit.bean;

import java.util.HashMap;
import java.util.Map;

public class AdShowLimitBean {
	private Map<String, Integer> AdShowLimitMap = new HashMap<String, Integer>();

	public Map<String, Integer> getAdShowLimitMap() {
		return AdShowLimitMap;
	}

	public void setAdShowLimitMap(Map<String, Integer> adShowLimitMap) {
		AdShowLimitMap = adShowLimitMap;
	}

}
