package com.pchome.dmp.enumerate;

public enum EnumDmpDailyReport {

		Ruten(
			"RUTEN",
			"goods.ruten"
				);

		private String site;
		private String urlPattern;

		private EnumDmpDailyReport(String site, String urlPattern) {
			this.site = site;
			this.urlPattern = urlPattern;
		}

		public String getSite() {
			return site;
		}

		public String getUrlPattern() {
			return urlPattern;
		}

}
