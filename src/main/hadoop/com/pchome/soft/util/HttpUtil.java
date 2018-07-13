package com.pchome.soft.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Character.UnicodeBlock;
import java.net.HttpURLConnection;
import java.net.IDN;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnPerRouteBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.util.EntityUtils;

/**
 * httpclient all settings
 * 
 * @author supertolove
 * 
 */
public class HttpUtil {
    private static final Log log = LogFactory.getLog(HttpUtil.class);
    private static HttpUtil http = new HttpUtil();
    private DefaultHttpClient client;

	public static HttpUtil getInstance() {
		return http;
	}
    
	public synchronized DefaultHttpClient getClient() {
		return client;
	}

	private HttpUtil() {
		try {
			configureClient();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    // Add URL Decode 2014/10/15 alex
	public synchronized String deCode(String url) throws Exception {
		log.info("deCode start");
		return URLDecoder.decode(url, "UTF-8");
	}

    // Add URL Encode 2014/10/15 alex
	public synchronized String enCode(String url) throws Exception {
		log.info("enCode start");
		String enCodeUrl = "";
		for (int i = 0; i <= url.trim().length() - 1; i++) {
			if (UnicodeBlock.of(url.charAt(i)).toString().equals("CJK_UNIFIED_IDEOGRAPHS")) {
				enCodeUrl = enCodeUrl + URLEncoder.encode(String.valueOf(url.charAt(i)), "UTF-8");
			} else {
				enCodeUrl = enCodeUrl + url.charAt(i);
			}
		}
		log.info("enCode end" + enCodeUrl);
		return enCodeUrl;
	}
	
    // Add URL ASCII Code 2014/10/15 反譯alex
	public synchronized String getUnicode(String domain) throws Exception {
		log.info("ASCII Code start");
		if (StringUtils.isBlank(domain) && domain.length() < 1) {
			return null;
		}
		return IDN.toUnicode(domain);
	}
	
    // Add URL RealUrl code 2014/10/15 編譯 alex
	public synchronized String getASCII(String domain) throws Exception {
		log.info("getUrlASCII start");
		if (StringUtils.isEmpty(domain) || domain.length() < 1) {
			return null;
		}
		return IDN.toASCII(domain);
	}
    
    // Add URL RealUrl code 2014/10/15 alex
	public synchronized String getRealUrl(String urlPath) throws Exception {
		log.info("getRealUrl start>>>" + urlPath);

		String returnUrl = "";

		if (StringUtils.isEmpty(urlPath) || urlPath.length() < 1) {
			// return null;
		}
		// urlPath ="http://收購老酒.tw/";
		URL url = new URL(urlPath);
		String urlDomain = getASCII(url.getHost());
		String path = "";
		if (url.getQuery() != null) {
			path = enCode(url.getPath() + "?" + url.getQuery());
		} else if (url.getRef() != null) { // 2016-12-02 增加
			path = enCode(url.getPath() + "#" + url.getRef());
		} else {
			path = enCode(url.getPath());
		}

		if (urlPath.indexOf("https://") == 0) {
			returnUrl = "https://";
		} else {
			returnUrl = "http://";
		}

		return returnUrl + urlDomain + path;
	}
    
	public synchronized String convertRealUrl(String urlPath) throws Exception {
		log.info("convertRealUrl start>>>" + urlPath);

		if (StringUtils.isEmpty(urlPath) || urlPath.length() < 1) {
			return null;
		}
		URL url = new URL(urlPath);
		String path = "";
		if (url.getQuery() != null) {
			path = deCode(url.getPath() + "?" + url.getQuery());
		} else if (url.getRef() != null) { // 2016-12-02 增加
			path = enCode(url.getPath() + "#" + url.getRef());
		} else {
			path = deCode(url.getPath());
		}
		return getUnicode(url.getHost()) + path;
	}
    
	public synchronized String getResult(String url, String charSet) {
		String result = null;

		HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK");
		HttpGet httpget = null;

		try {
			httpget = new HttpGet(url);
			response = client.execute(httpget);
			HttpEntity entity = response.getEntity();
			if (StringUtils.isBlank(charSet)) {
				result = EntityUtils.toString(entity);
			} else {
				result = EntityUtils.toString(entity, charSet);
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			httpget.abort();
			closeExpiredConns();
			closeIdleConns();
		}

		result = StringUtils.defaultIfEmpty(result, "");
		return result;
	}

	/**
	 * 回傳http 狀態碼
	 * @param url
	 * @return 狀態碼
	 * @throws IOException
	 */
	public synchronized String getResult(String url) throws IOException {
		String result = null;

		DefaultHttpClient httpClient = new DefaultHttpClient();

		HttpPost httpPost = new HttpPost(url);

		HttpResponse respones = httpClient.execute(httpPost);

		result = Integer.toString(respones.getStatusLine().getStatusCode());

		return result;
	}

	/**
	 * 用get方式取得資料
	 * @param url
	 * @return 取得資料, 狀態碼
	 * @throws IOException
	 */
	public synchronized StringBuffer doGet(String url) throws IOException {
		URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        //默認值GET
        con.setRequestMethod("GET");

        //增加 Header
        con.setRequestProperty("User-Agent", "Mozilla/5.0");

        int responseCode = con.getResponseCode();
//        System.out.println("\nSending 'GET' request to URL : " + url);
//        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
		return response.append(", status:" + responseCode);
//		return response;
	}
	
    /**
     * @author weich
     * @param String
     *            url
     * @return int statusCode
     * @throws Exception 
     */
	public synchronized int getStatusCode(String url) throws Exception {
		log.info(">>>>>>>>>>>>>>url=" + url);
		int statusCode = HttpStatus.SC_NOT_FOUND;
		url = getRealUrl(url);
		if (StringUtils.isNotEmpty(url)) {
			HttpGet httpget = null;
			try {
				URL thisUrl = new URL(url);
				URI uri = new URI(thisUrl.getProtocol(), thisUrl.getHost(), thisUrl.getPath(), thisUrl.getQuery(), null);
				httpget = new HttpGet(uri);

				// 禁止get自動處理重新定向
				/*
				 * HttpParams params = client.getParams();
				 * params.setParameter(ClientPNames.HANDLE_REDIRECTS, false);
				 */

				statusCode = client.execute(httpget).getStatusLine().getStatusCode();

				if (statusCode == HttpStatus.SC_FORBIDDEN) {
					statusCode = getURLConnectionStatus(url);
				}

			} catch (Exception e) {
				log.error(e.getMessage(), e);
				statusCode = getURLConnectionStatus(url);
			} finally {
				httpget.abort();
				closeExpiredConns();
				closeIdleConns();
			}
		}

		return statusCode;
	}

	private int getURLConnectionStatus(String url) {

		int statusCode = HttpStatus.SC_NOT_FOUND;

		try {
			String httpUrl = "http://";

			if (url.indexOf("https://") == 0) {
				httpUrl += url.substring(8);
			} else if (url.indexOf("http://") == 0) {
				httpUrl = url;
			} else {
				httpUrl += url;
			}

			URLConnection connection = new URL(httpUrl).openConnection();
			connection.setRequestProperty("User-Agent",
					"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
			connection.connect();
			BufferedReader r = new BufferedReader(
					new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = r.readLine()) != null) {
				sb.append(line);
			}
			if (sb != null) {
				statusCode = 200;
			}
		} catch (Exception a) {
			statusCode = HttpStatus.SC_NOT_FOUND;
		}

		return statusCode;
	}
    
	private void configureClient() throws NoSuchAlgorithmException, KeyManagementException {

		HttpParams params = new BasicHttpParams();
		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
		HttpProtocolParams.setContentCharset(params, "UTF-8");
		HttpProtocolParams.setUserAgent(params, "TestAgent/1.1");
		HttpProtocolParams.setUseExpectContinue(params, false);
		HttpConnectionParams.setStaleCheckingEnabled(params, false);
		ConnManagerParams.setMaxTotalConnections(params, 200);
		ConnManagerParams.setMaxConnectionsPerRoute(params, new ConnPerRouteBean(20));
		// 設定連接超時3秒
		HttpConnectionParams.setConnectionTimeout(params, 5 * 1000);
		HttpConnectionParams.setSoTimeout(params, 5 * 1000);

		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, new TrustManager[] { new X509TrustManager() {
			public void checkClientTrusted(X509Certificate[] chain, String authType) {

			}

			public void checkServerTrusted(X509Certificate[] chain, String authType) {

			}

			public X509Certificate[] getAcceptedIssuers() {
				return new X509Certificate[] {};
			}
		} }, new SecureRandom());

		// Create and initialize scheme registry
		SchemeRegistry schemeRegistry = new SchemeRegistry();
		SSLSocketFactory sf = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
		schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
		// schemeRegistry.register(new Scheme("https",
		// SSLSocketFactory.getSocketFactory(), 443));
		schemeRegistry.register(new Scheme("https", sf, 443));

		// Create an HttpClient with the ThreadSafeClientConnManager.
		// This connection manager must be used if more than one thread will
		// be using the HttpClient.
		ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);

		client = new DefaultHttpClient(cm, params);
		client.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(3, true));
	}

	public void shutdownHttp() {
		client.getConnectionManager().shutdown();
	}

	public void closeExpiredConns() {
		client.getConnectionManager().closeExpiredConnections();
	}

	public void closeIdleConns() {
		client.getConnectionManager().closeIdleConnections(10L, TimeUnit.SECONDS);
	}

	public static void main(String age[]) throws Exception {

		// try {
		// System.out.print("state : "
		// + httpUtil.getInstance().getResult(url, "UTF-8"));
		// } catch (Exception e) {
		// System.out.print(e.toString());
		//
		// }
	}
}