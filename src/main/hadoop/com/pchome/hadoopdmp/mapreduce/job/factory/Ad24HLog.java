package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;



public class Ad24HLog extends ACategoryLogData {
	Log log = LogFactory.getLog("Ad24HLog");
	private DBCollection dBCollection;
	private static String referer = "";
	private static String class24hUrlClassify = "";
	private static String category = "";
	private static String categorySource = "";
	private static List<CategoryCodeBean> list;
	private static DBObject dbObject = null;
	private static BasicDBObject andQuery = new BasicDBObject();
	private static List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
	private static BasicDBObject basicDBObject = new BasicDBObject();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static Map<String,String> urlCodeMapping = new HashedMap<String,String>();
	private static Map<String,DBObject> urlDBObjectMapping = new HashedMap<String,DBObject>();
	private static Date today = new Date();
	private static int totalcount = 0;
	public Object processCategory(net.minidev.json.JSONObject dmpJSon, DBCollection dbCollectionUrl) throws Exception {
//		log.info(">>>>>>>>>>>>>>>>>>>>>1");
		category = "";
		categorySource = "";
		class24hUrlClassify = "" ;
		this.dBCollection = dbCollectionUrl;
		this.referer = dmpJSon.getAsString("referer").trim();
		if (StringUtils.isBlank(referer)) {
//			dmpJSon.put("class_24h_url_classify", "N");
			dmpJSon.put("classify", "N");
			dmpJSon.put("behavior", "24h");
			return dmpJSon;
		}
//		log.info(">>>>>>>>>>>>>>>>>>>>>2");
		//用url比對24h對照表找出分類代號
//		list = DmpLogMapper.category24hBeanList;
		if(urlCodeMapping.containsKey(referer)) {
//			log.info(">>>>>>>>>>>>>>>>>>>>>3");
			category = urlCodeMapping.get(referer);
			if(StringUtils.isNotBlank(category)) {
				categorySource = "24h";
				class24hUrlClassify = "Y";
			}
		}else {
//			log.info(">>>>>>>>>>>>>>>>>>>>>4");
			for (CategoryCodeBean categoryBean : DmpLogMapper.category24hBeanList) {
				if(this.referer.indexOf(categoryBean.getEnglishCode()) != -1){
					category = categoryBean.getNumberCode();
					categorySource = "24h";
					class24hUrlClassify = "Y";
					urlCodeMapping.put(this.referer, category);
					break;
				}
			}
			urlCodeMapping.put(this.referer, "");
		}
		
		//	url比對不到24H分類
		if (StringUtils.isBlank(category)){
//			log.info(">>>>>>>>>>>>>>>>>>>>>5");
			//查詢url
			if(urlDBObjectMapping.containsKey(this.referer)) {
				dbObject = urlDBObjectMapping.get(this.referer);
			}else {
				dbObject = queryClassUrl(this.referer);
				if(dbObject.get("query_time") == null) {
					dbObject.put("query_time", new Integer(0));
				}
				urlDBObjectMapping.put(this.referer, dbObject);
			}
//			log.info(">>>>>>>>>>>>>>>>>>>>>5-1");
			if (dbObject != null) { //mongo db有資料
//				log.info(">>>>>>>>>>>>>>>>>>>>>5-2");
				if (dbObject.get("status").equals("0")) {
					category = "";
					categorySource = "";
					class24hUrlClassify = "N";
					// url 存在 status = 0 , mongo update_date 更新(一天一次) query_time+1 如大於 2000 不再加
					updateClassUrlUpdateDate(this.referer, dbObject);
					updateClassUrlQueryTime(this.referer, dbObject);
					dbObject.put("query_time", (Integer.parseInt(dbObject.get("query_time").toString()) + 1));
					urlDBObjectMapping.put(this.referer, dbObject);
				} else if ((dbObject.get("status").equals("1"))	&& (StringUtils.isNotBlank(dbObject.get("ad_class").toString()))) {
//					log.info(">>>>>>>>>>>>>>>>>>>>>5-3");
					category = dbObject.get("ad_class").toString();
					categorySource = "24h";
					class24hUrlClassify = "Y";
					// url 存在 status = 1 取分類代號回傳 mongo update_date 更新(一天一次) class24hUrlClassify =
					// "Y"
					updateClassUrlUpdateDate(this.referer, dbObject);
				}
			}else { //mongo db無資料
//				log.info(">>>>>>>>>>>>>>>>>>>>>5-4");
				category = "";
				categorySource = "";
				class24hUrlClassify = "N";
				// url 不存在 ,寫入 mongo url代號 status=0
				insertClassUrl(this.referer, "", "0", 1);
			}
		}
		dmpJSon.put("classify", class24hUrlClassify);
		dmpJSon.put("behavior", "24h");
		dmpJSon.put("category", category);
		return dmpJSon;
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
		String todayStr = sdf.format(today);
		String updateDateStr = sdf.format(dbObject.get("update_date"));
		if ((!todayStr.equals(updateDateStr)) ){
		    DBObject olddbObject = new BasicDBObject();
		    olddbObject.put("url", url);
		    dBCollection.update(olddbObject, dbObject);
		}
	}
	
	public void updateClassUrlQueryTime(String url,DBObject dbObject) throws Exception {
		if ((Integer.parseInt(dbObject.get("query_time").toString()) <2000) ){
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.append("$inc", new BasicDBObject().append("query_time", 1));
			DBObject filter = new BasicDBObject(); 
			filter.put("url", url);
			filter.put("_id", dbObject.get("_id"));
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