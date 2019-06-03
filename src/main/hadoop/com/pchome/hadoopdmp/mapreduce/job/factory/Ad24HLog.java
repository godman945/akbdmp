package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;


public class Ad24HLog extends ACategoryLogData {
	private DBCollection dBCollection;
	private static String sourceUrl = "";
	private static String class24hUrlClassify = "";
	private static String category = "";
	private static String categorySource = "";
	private static List<CategoryCodeBean> list;
	private static DBObject dbObject = null;
	private static BasicDBObject andQuery = new BasicDBObject();
	private static List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
	private static BasicDBObject basicDBObject = new BasicDBObject();
	
	private static Map<String,String> urlCodeMapping = new HashedMap<String,String>();
	private static Map<String,DBObject> urlDBObjectMapping = new HashedMap<String,DBObject>();
	public Object processCategory(net.minidev.json.JSONObject dmpJSon, DBCollection dbCollectionUrl) throws Exception {
		category = "";
		categorySource = "";
		class24hUrlClassify = "" ;
		this.dBCollection = dbCollectionUrl;
		sourceUrl = dmpJSon.getAsString("referer");
		if (StringUtils.isBlank(sourceUrl)) {
			dmpJSon.put("class_24h_url_classify", "N");
			return dmpJSon;
		}
		
		//用url比對24h對照表找出分類代號
		list = DmpLogMapper.category24hBeanList;
		if(urlCodeMapping.containsKey(sourceUrl)) {
			category = urlCodeMapping.get(sourceUrl);
			if(StringUtils.isNotBlank(category)) {
				categorySource = "24h";
				class24hUrlClassify = "Y";
			}
		}else {
			for (CategoryCodeBean categoryBean : list) {
				if(sourceUrl.indexOf(categoryBean.getEnglishCode()) != -1){
					category = categoryBean.getNumberCode();
					categorySource = "24h";
					class24hUrlClassify = "Y";
					urlCodeMapping.put(sourceUrl, category);
					break;
				}
			}
			urlCodeMapping.put(sourceUrl, "");
		}
		
		
		if (StringUtils.isBlank(category)){
			//查詢url
			if(urlDBObjectMapping.containsKey(sourceUrl.trim())) {
				dbObject = urlDBObjectMapping.get(sourceUrl.trim());
			}else {
				dbObject = queryClassUrl(sourceUrl.trim());
				urlDBObjectMapping.put(sourceUrl.trim(), dbObject);
			}
			
			if (dbObject != null) {
				if (dbObject.get("status").equals("0")) {
					category = "";
					categorySource = "";
					class24hUrlClassify = "N";
					// url 存在 status = 0 , mongo update_date 更新(一天一次) query_time+1 如大於 2000 不再加
					updateClassUrlUpdateDate(sourceUrl.trim(), dbObject);
					updateClassUrlQueryTime(sourceUrl.trim(), dbObject);
				} else if ((dbObject.get("status").equals("1"))	&& (StringUtils.isNotBlank(dbObject.get("ad_class").toString()))) {
					category = dbObject.get("ad_class").toString();
					categorySource = "24h";
					class24hUrlClassify = "Y";
					// url 存在 status = 1 取分類代號回傳 mongo update_date 更新(一天一次) class24hUrlClassify =
					// "Y"
					updateClassUrlUpdateDate(sourceUrl.trim(), dbObject);
				}
			}else {
				category = "";
				categorySource = "";
				class24hUrlClassify = "N";
				// url 不存在 ,寫入 mongo url代號 status=0
				insertClassUrl(sourceUrl.trim(), "", "0", 1);
			}
		}
		
		dmpJSon.put("category", category);
		dmpJSon.put("category_source", categorySource);
		dmpJSon.put("class_24h_url_classify", class24hUrlClassify);
		return dmpJSon;
		
		
		
		
		
		
//		this.dBCollection= mongoOperations.getCollection("class_url");
//		
//		dmpDataBean.setSource("kdcl");
//		
//		String sourceUrl = dmpDataBean.getUrl();
//		String category = "null";
//		String categorySource = "null";
//		String class24hUrlClassify = "null";
//		
//		if (StringUtils.isBlank(sourceUrl)) {
//			dmpDataBean.setUrl("null");
//			dmpDataBean.setCategory("null");
//			dmpDataBean.setCategorySource("null");
//			dmpDataBean.setClass24hUrlClassify("N");
//			return dmpDataBean;
//		}
//		
//		//用url比對24h對照表找出分類代號
//		List<CategoryCodeBean> list = DmpLogMapper.category24hBeanList;
//		for (CategoryCodeBean categoryBean : list) {
//			if(sourceUrl.indexOf(categoryBean.getEnglishCode()) != -1){
//				category = categoryBean.getNumberCode();
//				categorySource = "24h";
//				class24hUrlClassify = "Y";
//				break;
//			}
//		}
//		
//		if (StringUtils.isBlank(category)){
//			//查詢url
//			DBObject dbObject =queryClassUrl(sourceUrl.trim()) ;
//			
//			if(dbObject != null){
//				if(dbObject.get("status").equals("0")){
//					category ="null";
//					categorySource = "null";
//					class24hUrlClassify = "N";
//					// url 存在 status = 0  , mongo update_date 更新(一天一次) query_time+1 如大於 2000 不再加 
//					updateClassUrlUpdateDate(sourceUrl.trim(),dbObject) ;
//					updateClassUrlQueryTime( sourceUrl.trim(),dbObject) ;
//				}else if( (dbObject.get("status").equals("1")) && (StringUtils.isNotBlank(dbObject.get("ad_class").toString())) ){
//					category = dbObject.get("ad_class").toString();
//					categorySource = "24h";
//					class24hUrlClassify = "Y"; 
//					//url 存在 status = 1 取分類代號回傳 mongo update_date 更新(一天一次) class24hUrlClassify = "Y"
//					updateClassUrlUpdateDate(sourceUrl.trim(),dbObject) ;
//				}
//			}else{
//				category = "null";
//				categorySource = "null";
//				class24hUrlClassify = "N";
//				// url 不存在  ,寫入 mongo url代號 status=0 
//				insertClassUrl(sourceUrl.trim(),"","0",1);
//			}
//		}
//		
//		dmpDataBean.setCategory(category);
//		dmpDataBean.setCategorySource(categorySource);
//		dmpDataBean.setClass24hUrlClassify(class24hUrlClassify);
//		
//		return dmpDataBean;
	}
	
	

	public DBObject queryClassUrl(String url) throws Exception {
		andQuery.clear();
		obj.clear();
		basicDBObject.clear();
		obj.add(basicDBObject.append("url",url));
		andQuery.put("$and", obj);
		return dBCollection.findOne(andQuery);
	}
	
	public void updateClassUrlUpdateDate(String url,DBObject dbObject) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todayStr = sdf.format(today);
		String updateDateStr = sdf.format(dbObject.get("update_date"));
		if ( (!todayStr.equals(updateDateStr)) ){
			Date date = new Date();
			dbObject.put("update_date", date);

		    DBObject olddbObject = new BasicDBObject();
		    olddbObject.put("url", url);
		    dBCollection.update(olddbObject, dbObject);
		}
	}
	
	public void updateClassUrlQueryTime(String url,DBObject dbObject) throws Exception {
		if ( (Integer.parseInt( dbObject.get("query_time").toString()) <2000) ){
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.append("$inc", new BasicDBObject().append("query_time", 1));
			DBObject filter = new BasicDBObject(); 
			filter.put("url", url);
			dBCollection.update(filter,newDocument);
		}
	}
	
	public void insertClassUrl(String url, String ad_class, String status,int query_time) throws Exception {
		Date today = new Date();
		DBObject documents = new BasicDBObject("url", url)
				.append("ad_class", ad_class)
				.append("status", status)
				.append("query_time", query_time)
				.append("create_date", today)
				.append("update_date", today);
		dBCollection.insert(documents);
	}
	
}