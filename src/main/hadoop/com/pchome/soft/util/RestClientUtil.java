package com.pchome.soft.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

@Component
//@Scope("prototype")
public class RestClientUtil {

	private RestTemplate rest;
	private HttpHeaders headers;
	private HttpStatus status;

	Log log = LogFactory.getLog(RestClientUtil.class);

	public RestClientUtil() {
		SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		requestFactory.setReadTimeout(50000);
		requestFactory.setConnectTimeout(50000);
		this.rest = new RestTemplate();
		this.headers = new HttpHeaders();
		headers.add("Content-Type", "application/json; charset=UTF-8");
		headers.add("Accept", "*/*");
		headers.add("Accept-Language", "zh-TW");
		rest.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
		rest.setRequestFactory(requestFactory);
		enableSSL();
	}

	public String get(String server, String uri) {
		HttpEntity<String> requestEntity = new HttpEntity<String>("", headers);
		ResponseEntity<String> responseEntity = rest.exchange(server + uri, HttpMethod.GET, requestEntity,	String.class);
		this.setStatus(responseEntity.getStatusCode());
		return responseEntity.getBody();
	}
	
	/**
	 * 探訪需要帳密網頁權限
	 * */
	@SuppressWarnings("deprecation")
	
//	public String getByBasicCredentialsProvider(String server, String uri,String account,String password) {
//		DefaultHttpClient httpClient = new DefaultHttpClient();
//		BasicCredentialsProvider credentialsProvider =  new BasicCredentialsProvider();
//		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(account, password));
//		httpClient.setCredentialsProvider(credentialsProvider);
//		ClientHttpRequestFactory rf = new HttpComponentsClientHttpRequestFactory(httpClient);
//		rest = new RestTemplate(rf);
//		return rest.getForObject(server, String.class);
//	}
	
	public String post(String server, @SuppressWarnings("rawtypes") MultiValueMap multiValueMap) {
		HttpEntity<String> requestEntity = new HttpEntity<String>("", headers);
		ResponseEntity<String> responseEntity = rest.postForEntity(server , multiValueMap, String.class);
		return responseEntity.getBody();
	}
	
	
	public String postStringEntity(String server, String data) throws Exception{
		HttpEntity<String> requestEntity = new HttpEntity<String>(data, headers);
		ResponseEntity<String> responseEntity = rest.exchange(server, HttpMethod.POST, requestEntity,String.class);
		return responseEntity.getBody();
	}
	
	
	
	public void put(String server, String uri, String json) {
		HttpEntity<String> requestEntity = new HttpEntity<String>(json, headers);
		ResponseEntity<String> responseEntity = rest.exchange(server + uri, HttpMethod.PUT, requestEntity,
				String.class);
		this.setStatus(responseEntity.getStatusCode());
	}

	public String putFile(String server, String uri, String filePath) throws IOException {
		Resource resource = new FileSystemResource(filePath);
		return putResource(server, uri, resource);
	}

	public String putUrlFile(String server, String uri, String fileUrlPath) throws MalformedURLException, IOException {
		log.info("rest putUrlFile fuction");

		Resource resource = new UrlResource(fileUrlPath);
		return putResource(server, uri, resource);

	}

	public String putByteArray(String server, String uri, byte[] byteFile) throws MalformedURLException, IOException {

		log.info("rest putByteArray fuction");

		Resource resource = new ByteArrayResource(byteFile);
		return putResource(server, uri, resource);

	}

	public String putResource(String server, String uri, Resource resource) throws MalformedURLException, IOException {

		// log.info("rest putResource fuction");

		MultiValueMap<String, Object> parts = new LinkedMultiValueMap<String, Object>();
		// parts.add("Content-Type", "image/jpg");
		parts.add("file", resource);
		parts.add("id_product", uri);

		HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<MultiValueMap<String, Object>>(parts);
		ResponseEntity<String> responseEntity = rest.exchange(server + uri, HttpMethod.PUT, requestEntity,String.class);
		this.setStatus(responseEntity.getStatusCode());
		return responseEntity.getBody();

	}

	public void delete(String server, String uri) {
		HttpEntity<String> requestEntity = new HttpEntity<String>("", headers);
		ResponseEntity<String> responseEntity = rest.exchange(server + uri, HttpMethod.DELETE, requestEntity,
				String.class);
		this.setStatus(responseEntity.getStatusCode());
	}

	public HttpStatus getStatus() {
		return status;
	}

	public void setStatus(HttpStatus status) {
		this.status = status;
	}

	/**
	 * 略過SSL
	 * */
	private void enableSSL() {
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
			}

			public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
			}
		} };
		try {
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		} catch (Exception e) {
			log.error(">>>>"+e.getMessage());
		}
	}

	public RestTemplate getRest() {
		return this.rest;
	}
}
