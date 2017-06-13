package com.pchome.dmp.mapreduce.job.factory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

public class PersonalMemberInfo extends APersonalInfo {

	public Object personalData(Map<String, Object> map) throws Exception {
		StringBuffer url = new StringBuffer();
		url.append("http://member.pchome.com.tw/findMemberInfo4ADAPI.html?ad_user_id=");
		url.append(map.get("memid"));
		String prsnlData = httpGet(url.toString());

		// 取得sex,age
		return null;
	}

	public String httpGet(String myURL) {
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		InputStreamReader in = null;
		try {
			URL url = new URL(myURL);
			urlConn = url.openConnection();
			if (urlConn != null)
				urlConn.setReadTimeout(60 * 1000);
			if (urlConn != null && urlConn.getInputStream() != null) {
				in = new InputStreamReader(urlConn.getInputStream(), "UTF-8");
				BufferedReader bufferedReader = new BufferedReader(in);
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			}
			in.close();
		} catch (Exception e) {
			// log.error(e.getMessage());
		}

		return sb.toString();
	}

}