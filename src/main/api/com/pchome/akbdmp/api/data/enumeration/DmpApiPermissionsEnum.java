package com.pchome.akbdmp.api.data.enumeration;

	public enum DmpApiPermissionsEnum {
		API_01("adShowLimit", true),
		API_02("LifeCheck", true),
		API_03("adclassApi", true),
		API_04("dmpInfoApi", true),
		API_05("prodAdTest", true),
		API_06("deleteDmp", true);
		

		private final String method;
		private final boolean approve;
		private DmpApiPermissionsEnum(String method, boolean approve) {
			this.method = method;
			this.approve = approve;
		 }
		public String getMethod() {
			return method;
		}
		public boolean isApprove() {
			return approve;
		}
}

	