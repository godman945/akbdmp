package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.enumerate.EnumBreadCrumbDirectlyMatch;
import com.pchome.hadoopdmp.enumerate.PersonalInfoEnum;


@SuppressWarnings("unchecked")
public class Ad24HLog extends ACategoryLogData {

	
	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {

		String memid = values[1];
		String uuid = values[2];
		String sourceUrl = values[4];
		String adClass = "";
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null"))) {
			return null;
		}

		if (StringUtils.isBlank(sourceUrl)) {
			return null;
		}
		
		ClassUrlMongoBean classUrlMongoBean = null;
		Query query = new Query(Criteria.where("url").is(sourceUrl.trim()));
		classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
		if (classUrlMongoBean == null){
			adClass = crawlerGetAdclass(categoryLogBean,sourceUrl);
			Date date = new Date();
			ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
			classUrlMongoBeanCreate.setAd_class(adClass);
			classUrlMongoBeanCreate.setStatus(StringUtils.isBlank(adClass) ? "0" : "1");
			classUrlMongoBeanCreate.setUrl(sourceUrl); 
			classUrlMongoBeanCreate.setCreate_date(date);
			classUrlMongoBeanCreate.setUpdate_dateDate(date);
			mongoOperations.save(classUrlMongoBean);
		}
		
		if(classUrlMongoBean != null){
			//爬蟲
			if(classUrlMongoBean.getStatus() == "0"){
				adClass = crawlerGetAdclass(categoryLogBean,sourceUrl);
				if(StringUtils.isNotBlank(adClass)){
					Date date = new Date();
					classUrlMongoBean.setStatus("1");
					classUrlMongoBean.setUpdate_dateDate(date);
					mongoOperations.save(classUrlMongoBean);
				}
			}
			
			//比對個資
			if(classUrlMongoBean.getStatus() == "0"){
				adClass = classUrlMongoBean.getAd_class();
			}
		}
		
		//1.enum比對不到且爬蟲也沒有
		if(StringUtils.isBlank(adClass)){
			return null;
		}
		
		//2取個資
		if(StringUtils.isNotBlank(memid)){
			APersonalInfo aPersonalInfo = PersonalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.MEMBER);
			Map<String, Object> memberMap = aPersonalInfo.getMap();
			memberMap.put("memid", memid);
			
			Map<String, Object> userInfo = (Map<String, Object>) aPersonalInfo.personalData(memberMap);
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
//			categoryLogBean.setRecodeDate(recodeDate);
			categoryLogBean.setSource("24h");
			categoryLogBean.setSex(StringUtils.isNotBlank(userInfo.get("sex").toString()) ? userInfo.get("sex").toString(): "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(userInfo.get("age").toString()) ? userInfo.get("age").toString(): "null");
			return categoryLogBean;
		}else if(StringUtils.isNotBlank(uuid)){
			APersonalInfo aPersonalInfo = PersonalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.UUID);
			Map<String, Object> uuidMap = aPersonalInfo.getMap();
			uuidMap.put("adClass", adClass);
			uuidMap.put("ClsfyCraspMap", categoryLogBean.getClsfyCraspMap());
			Map<String, Object> userInfo = (Map<String, Object>) aPersonalInfo.personalData(uuidMap);
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
//			categoryLogBean.setRecodeDate(recodeDate);
			categoryLogBean.setSource("24h");
			categoryLogBean.setSex(StringUtils.isNotBlank(userInfo.get("sex").toString()) ? userInfo.get("sex").toString(): "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(userInfo.get("age").toString()) ? userInfo.get("age").toString(): "null");
			return categoryLogBean;
		}

		
		
		
		
		
		
		
		
		
		
		
//		ClassUrlMongoBean classUrlMongoBean = null;
//		Query query = new Query(Criteria.where("_id").is("59419cede4b054a874e9c442"));
//		classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
//		if(classUrlMongoBean == null){
//			classUrlMongoBean = new ClassUrlMongoBean();
//			classUrlMongoBean.setAd_class("aaa");
//			classUrlMongoBean.setStatus("0");;
//			classUrlMongoBean.setCreate_date(new Date());
//			classUrlMongoBean.setUrl("123.com");
//			classUrlMongoBean.setUpdate_dateDate(new Date());
//			mongoOperations.save(ClassUrlMongoBean.class);
//		}
//		
		
//		MongoTemplate mongoTemplate = (MongoTemplate) mongoOperations;
		
//		ClassUrlMongoBean classUrlMongoBean2 = new ClassUrlMongoBean();
//		classUrlMongoBean2.setAd_class("aaa");
////		classUrlMongoBean2.setStatus("0");
////		classUrlMongoBean2.setCreate_date(new Date());
////		classUrlMongoBean2.setUrl("123.com");
////		classUrlMongoBean2.setUpdate_dateDate(new Date());
////		mongoOperations.save(ClassUrlMongoBean.class);
//		mongoOperations.save(classUrlMongoBean2);
		
//		Date date = new Date();
//		ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//		classUrlMongoBeanCreate.setAd_class(adClass);
//		classUrlMongoBeanCreate.setStatus(StringUtils.isBlank(adClass) ? "0" : "1");
//		classUrlMongoBeanCreate.setUrl(sourceUrl); 
//		classUrlMongoBeanCreate.setCreate_date(date);
//		classUrlMongoBeanCreate.setUpdate_dateDate(date);
//		mongoOperations.save(classUrlMongoBean);
		


		return null;
	}

	@SuppressWarnings("deprecation")
	public static NameValuePair requestGetAPI42(String uri) throws Exception {

		HttpParams httpparameters = new BasicHttpParams();
		HttpConnectionParams.setConnectionTimeout(httpparameters, 3000);
		HttpConnectionParams.setSoTimeout(httpparameters, 5000);

		HttpClient client = new DefaultHttpClient(httpparameters);
		// HttpClient client = new DefaultHttpClient();
		HttpResponse response;
		try {
			// get
			HttpGet request = new HttpGet(uri);

			// execute request => response
			response = client.execute(request);

			HttpEntity entity = response.getEntity();
			String resp = EntityUtils.toString(entity, "UTF-8");
			EntityUtils.consume(entity);

			NameValuePair result = new BasicNameValuePair(String.valueOf(response.getStatusLine().getStatusCode()),	resp);
			return result;
		} finally {
		
		}
	}

	
	
	
	
	public String crawlerGetAdclass(CategoryLogBean categoryLogBean,String sourceUrl) throws Exception{
		String adClass="";
		Map<String, String> map = new LinkedHashMap<String, String>();
		Map<String, String> oriMatchMap = new HashMap<String, String>();
		ArrayList<Map<String, String>> matchList = categoryLogBean.getList();
		for (int i = 0; i < matchList.size(); i++) {
			map.putAll(matchList.get(i));
			oriMatchMap.putAll(matchList.get(i));
		}

		// delete adult
		map.remove("0025000000000000");
		oriMatchMap.remove("0025000000000000");

		// modify for matching
		for (Map.Entry<String, String> entry : map.entrySet()) {
			entry.setValue(entry.getValue().replaceAll("/", "\u3001").replaceAll(" ", "\u3001")).replaceAll("\u3001\u3001\u3001", "\u3001").replaceAll("\u3001\u3001", "\u3001");
		}
		String url = sourceUrl.trim();

		String type;
		Pattern p = Pattern.compile("(http|https)://24h.pchome.com.tw/(store|region)/([a-zA-Z0-9]+)([&|\\?|\\.]\\S*)?");
		Matcher m = p.matcher(url);
		if (m.find()) {
			String mGrp3 = m.group(3);
			if (StringUtils.isNotBlank(m.group(3))) {
				type = mGrp3;
			} else {
				return null;
			}
		} else {
			return null;
		}

		StringBuffer urlTarget = new StringBuffer();
		urlTarget.append("http://ecapi.pchome.com.tw/cdn/ecshop/cateapi/v1.5/region&region=");
		urlTarget.append(type);
		urlTarget.append("&_callback=cb_ecshopCategoryRegion");
		NameValuePair result = requestGetAPI42(urlTarget.toString());
		String content;
		if (result != null && StringUtils.isNotBlank(result.getValue())) {
			content = result.getValue();
		} else {
			return null;
		}

		Pattern p2 = Pattern.compile("\"Name\"[ :]+((?=\\[)\\[[^]]*\\]|(?=\\{)\\{[^\\}]*\\}|\\\"[^\"]*\\\")");
		Matcher m2 = p2.matcher(content);
		String breadCrumb;
		String orig_breadCrumb;
		if (m2.find()) {
			orig_breadCrumb = m2.group(1).replaceAll("\"", "");
			breadCrumb = m2.group(0).replaceAll("\"", "").replaceAll("@", "").replaceAll(" ", "").replaceAll("\u3000", "").replace("Name:", "").trim();
		} else {
			return null;
		}

		orig_breadCrumb = StringEscapeUtils.unescapeJava(orig_breadCrumb);
		breadCrumb = StringEscapeUtils.unescapeJava(breadCrumb);
		breadCrumb = breadCrumb.replaceAll("\"", "").replaceAll("@", "").replaceAll(" ", "").replaceAll("\u3000", "");

		adClass = "";
		// BreadCrumb Directly Match
		for (EnumBreadCrumbDirectlyMatch item : EnumBreadCrumbDirectlyMatch.values()) {
			if (orig_breadCrumb.matches(item.getMatchPattern())) {
				adClass = item.getAdClass();
				break;
			}
		}

		if (StringUtils.isBlank(adClass)) {
			// step1
			String normalizeBreadCrumb = breadCrumb.replaceAll("\u5176\u4ed6", "").replaceAll("\u5176\u5b83", "").replaceAll("\u7528\u54c1", "").replaceAll("\u5de5\u5177", "").replaceAll("\u9031\u908a", "");
			// step2
			normalizeBreadCrumb = normalizeBreadCrumb.replaceAll("/", ",").replaceAll("\\.", ",").replaceAll(",,,,", ",").replaceAll(",,,", ",").replaceAll(",,", ",");
			if (normalizeBreadCrumb.endsWith(",")) {
				normalizeBreadCrumb = normalizeBreadCrumb.substring(0, normalizeBreadCrumb.length() - 1);
			}
			StringBuffer rdyToSplit = new StringBuffer().append(normalizeBreadCrumb);
			// Split breadcrumb into keywords
			// 全英數不補切
			if (normalizeBreadCrumb.matches("[a-zA-Z0-9|,]*")) {
				// log.info("normalizeBreadCrumb is all alphanumeric ,
			}
			// 含中英，補切中，英
			else if (normalizeBreadCrumb.matches(".*[\u4E00-\u9FFF]+.*")
					&& normalizeBreadCrumb.matches(".*[a-zA-Z0-9]+.*")) {
				String commaed = ChiEngComma(normalizeBreadCrumb);
				if (commaed.matches("\\S+,\\S+")) {
					rdyToSplit.append(",");
					rdyToSplit.append(commaed);
				} else {
					// log.warn("commaed error occured, normalizeBreadCrumb:" +
					// normalizeBreadCrumb);
				}
			}
			// 其餘照這補切
			else {
				String nmlzDeCommaStr = normalizeBreadCrumb.replaceAll(",", "");
				if (nmlzDeCommaStr.length() == 3) {
					rdyToSplit.append(",");
					rdyToSplit.append(nmlzDeCommaStr.substring(0, 2));
				}
				if (nmlzDeCommaStr.length() == 4) {
					rdyToSplit.append(",");
					rdyToSplit.append(nmlzDeCommaStr.substring(0, 2));
					rdyToSplit.append(",");
					rdyToSplit.append(nmlzDeCommaStr.substring(2, 4));
				}
				if (nmlzDeCommaStr.length() == 5) {
					rdyToSplit.append(",");
					rdyToSplit.append(nmlzDeCommaStr.substring(0, 2));
					rdyToSplit.append(",");
					rdyToSplit.append(nmlzDeCommaStr.substring(2, 5));
				}
			}

			String[] kwStrArray = rdyToSplit.toString().split(",");

			adClass = likeStringSearchV2(map, kwStrArray);
			adClass = StringUtils.isBlank(adClass) ? "unclassed" : adClass;

			// second time (Broden&Extreme)
			// 之前寫法,暫不異動但看似條件這樣下永遠不會進入以下判斷式
			Boolean extremeWay = false;
			String urlCated2nd = "";
			if (extremeWay && adClass.equals("unclassed")) {
				kwStrArray = normalizeBreadCrumb.replaceAll(",", "").split("");
				urlCated2nd = likeStringSearchV2(map, kwStrArray);
				urlCated2nd = StringUtils.isBlank(urlCated2nd) ? "unclassed" : urlCated2nd;
			}
		}
		
		return adClass;
	}
	
	
	
	public String ChiEngComma(String str) {
		String[] strArray = str.split("");
		List<String> chi = new ArrayList<String>();
		List<String> eng = new ArrayList<String>();
		for (String s : strArray) {
			if (s.matches("[\u4E00-\u9FFF]{1}")) {
				chi.add(s);
			} else if (s.matches("[a-zA-Z0-9]{1}")) {
				eng.add(s);
			}
		}
		StringBuffer strBuf = new StringBuffer();
		for (String s : chi) {
			strBuf.append(s);
		}
		strBuf.append(",");
		for (String s : eng) {
			strBuf.append(s);
		}
		return strBuf.toString();
	}

	/*-------------------*/
	/*------ Match ------*/
	public String likeStringSearchV2(Map<String, String> map, String[] kwCombi) throws Exception {
		float maxScore = 0;
		String maxScoreAdClass = "";
		float score = 0;
		for (Map.Entry<String, String> entry : map.entrySet()) {
			score = 0;
			String[] highHit = null;
			if (entry.getValue().contains("\u3001")) {
				highHit = entry.getValue().split("\u3001");
			}

			for (String str : kwCombi) {
				if (highHit != null) {
					for (String strH : highHit) {
						if (strH.equals(str)) {
							// log.info("High Hit , str:" + str + " , strH:" +
							// strH + " , return this adClass:" +
							// entry.getKey());
							return entry.getKey();
						}
						// log.info("HighHit not hit, strH:" + strH + " ,str:" +
						// str);
					}
				}
				if (entry.getValue().contains(str)) {
					score = score + 1;
					score = score + (1 - java.lang.Math.abs((float) (entry.getValue().length() - str.length()) / 10));
				}
			}
			if (score > maxScore) {
				maxScore = score;
				maxScoreAdClass = entry.getKey();
			}
		}
		return maxScoreAdClass;
	}
	/*------ Match ------*/
	/*-------------------*/
}