package com.pchome.akbdmp.api.data.enumeration;

public enum DmpApiPermissionsIPEnum {
	IP_1("192.168.1.1"),
	IP_2("0:0:0:0:0:0:0:1");
	

	private final String ip;

	private DmpApiPermissionsIPEnum(String ip) {
		this.ip = ip;
	}

	public String getIp() {
		return ip;
	}

}
