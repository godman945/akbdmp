package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;


public class Ad24HLog extends ACategoryLogData {
	Log log = LogFactory.getLog("Ad24HLog");
	private DBCollection dBCollection;
	
	public Object processCategory(DmpLogBean dmpDataBean, DB mongoOperations) throws Exception {
		try {
		
		this.dBCollection= mongoOperations.getCollection("class_url");
		
		dmpDataBean.setSource("kdcl");
		
		String sourceUrl = dmpDataBean.getUrl();
		String category = "null";
		String categorySource = "null";
		String class24hUrlClassify = "null";
		
		
		
		if (StringUtils.equals(dmpDataBean.getMemid(), "hihiboys")){
			log.info(">>>>>> memid : " + dmpDataBean.getMemid() );
			log.info(">>>>>> uuid : " + dmpDataBean.getUuid() );
			log.info(">>>>>> url : " + dmpDataBean.getUrl() );
		}
		
		
		if (StringUtils.isBlank(sourceUrl)) {
			dmpDataBean.setUrl("null");
			dmpDataBean.setCategory("null");
			dmpDataBean.setCategorySource("null");
			dmpDataBean.setClass24hUrlClassify("null");
			return dmpDataBean;
		}
		
		//用url比對24h對照表找出分類代號
		List<CategoryCodeBean> list = DmpLogMapper.category24hBeanList;
		for (CategoryCodeBean categoryBean : list) {
			if(sourceUrl.indexOf(categoryBean.getEnglishCode()) != -1){
				category = categoryBean.getNumberCode();
				categorySource = "24h";
				class24hUrlClassify = "Y";
				
				if (StringUtils.equals(dmpDataBean.getMemid(), "hihiboys")){
					log.info(">>>>>> 24h category : " + category );
				}
				
				break;
			}
		}
		
		if (StringUtils.isBlank(category)){
			//查詢url
			DBObject dbObject =queryClassUrl(sourceUrl.trim()) ;
			
			if(dbObject != null){
				if(dbObject.get("status").equals("0")){
					category ="null";
					categorySource = "null";
					class24hUrlClassify = "N";
					// url 存在 status = 0  , mongo update_date 更新(一天一次) query_time+1 如大於 2000 不再加 
					updateClassUrlUpdateDate(sourceUrl.trim(),dbObject) ;
					updateClassUrlQueryTime( sourceUrl.trim(),dbObject) ;
				}else if( (dbObject.get("status").equals("1")) && (StringUtils.isNotBlank(dbObject.get("ad_class").toString())) ){
					category = dbObject.get("ad_class").toString();
					categorySource = "24h";
					class24hUrlClassify = "Y"; 
					//url 存在 status = 1 取分類代號回傳 mongo update_date 更新(一天一次) class24hUrlClassify = "Y"
					updateClassUrlUpdateDate(sourceUrl.trim(),dbObject) ;
				}
			}else{
				category = "null";
				categorySource = "null";
				class24hUrlClassify = "N";
				// url 不存在  ,寫入 mongo url代號 status=0 
				insertClassUrl(sourceUrl.trim(),"","0",1);
			}
		}
		
		dmpDataBean.setCategory(category);
		dmpDataBean.setCategorySource(categorySource);
		dmpDataBean.setClass24hUrlClassify(class24hUrlClassify);
		
		} catch (Exception e) {
			log.error("24H category error>>>>>> " +e); 
		}
		
		return dmpDataBean;
	}
	
	

	public DBObject queryClassUrl(String url) throws Exception {
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("url",url));
		andQuery.put("$and", obj);
		DBObject dbObject =  dBCollection.findOne(andQuery);
		return dbObject;
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