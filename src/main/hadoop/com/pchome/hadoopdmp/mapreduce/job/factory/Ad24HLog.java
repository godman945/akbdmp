package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.dao.IClassUrlDAO;
import com.pchome.hadoopdmp.dao.mongodb.ClassUrlDAO;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;


public class Ad24HLog extends ACategoryLogData {
	private IClassUrlDAO daoClassUrl = ClassUrlDAO.getInstance();
	private DBCollection dBCollection;
	
	public Object processCategory(DmpLogBean dmpDataBean, DB mongoOperations) throws Exception {
		this.dBCollection= mongoOperations.getCollection("class_url");
		
		dmpDataBean.setSource("kdcl");
		
		String sourceUrl = dmpDataBean.getUrl();
		String category = "";
		String categorySource = "";
		String class24hUrlClassify = "";
		
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
				break;
			}
		}
		
		if (StringUtils.isBlank(category)){
//			DBCollection dBCollection = mongoOperations.getCollection("class_url");
//			BasicDBObject andQuery = new BasicDBObject();
//			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
//			obj.add(new BasicDBObject("url", sourceUrl.trim()));
//			andQuery.put("$and", obj);
//			DBObject dbObject =  dBCollection.findOne(andQuery);
			
			DBObject dbObject =queryClassUrl( mongoOperations,url) ;
			
			if(dbObject != null){
				if(dbObject.get("status").equals("0")){
					// url 存在 status = 0  , mongo update_date 更新(一天一次) query_time+1 如大於 2000 不再加 
					category ="null";
					categorySource = "null";
					class24hUrlClassify = "N";
					
					updateClassUrl(mongoOperations,url,dbObject) ;
					
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Date today = new Date();
					String todayStr = sdf.format(today);
//					Date updateDate = dbObject.get("update_date");
					String updateDateStr = sdf.format(dbObject.get("update_date"));
					if ( (!todayStr.equals(updateDateStr)) ){
						Date date = new Date();
						dbObject.put("update_date", date);

					    DBObject olddbObject = new BasicDBObject();
					    olddbObject.put("url", sourceUrl.trim());
					    dBCollection.update(olddbObject, dbObject);
//						classUrlMongoBean.setUpdate_date(date);		//old
//						mongoOperations.save(classUrlMongoBean);	//old
					}
					if ( (Integer.parseInt( dbObject.get("query_time").toString()) <2000) ){
//						Update querytime = new Update();
//						querytime.inc( "query_time" , 1 );
//						mongoOperations.updateFirst(new Query(Criteria.where( "url" ).is(sourceUrl.trim())), querytime, "class_url");
						
						BasicDBObject newDocument = new BasicDBObject();
						newDocument.append("$inc", new BasicDBObject().append("query_time", 1));
						DBObject filter = new BasicDBObject(); 
						filter.put("url", sourceUrl.trim());
						dBCollection.update(filter,newDocument);
					}
					
					
					
					
				}else if( (dbObject.get("status").equals("1")) && (StringUtils.isNotBlank(dbObject.get("ad_class").toString())) ){
					//url 存在 status = 1 取分類代號回傳 mongo update_date 更新(一天一次) class24hUrlClassify = "Y"
					category = dbObject.get("ad_class").toString();
					categorySource = "24h";
					class24hUrlClassify = "Y"; 
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					Date today = new Date();
					String todayStr = sdf.format(today);
					
					String updateDateStr = sdf.format(dbObject.get("update_date"));
//					Date updateDate = classUrlMongoBean.getUpdate_date();
//					String updateDateStr = sdf.format(updateDate);
					
					if ( (!todayStr.equals(updateDateStr)) ){
						Date date = new Date();
						dbObject.put("update_date", date);

					    DBObject olddbObject = new BasicDBObject();
					    olddbObject.put("url", sourceUrl.trim());
					    dBCollection.update(olddbObject, dbObject);
						
//						classUrlMongoBean.setUpdate_date(today);
//						mongoOperations.save(classUrlMongoBean);
					}
				}
			}else{
				// url 不存在  ,寫入 mongo url代號 status=0 
				category = "null";
				categorySource = "null";
				class24hUrlClassify = "N";
				
//				Date date = new Date();
//				ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//				classUrlMongoBeanCreate.setUrl(sourceUrl.trim());
//				classUrlMongoBeanCreate.setAd_class("");
//				classUrlMongoBeanCreate.setStatus("0");
//				classUrlMongoBeanCreate.setQuery_time(1);
//				classUrlMongoBeanCreate.setCreate_date(date);
//				classUrlMongoBeanCreate.setUpdate_date(date);
//				mongoOperations.save(classUrlMongoBeanCreate);
				
				
				Date today = new Date();
				DBObject documents = new BasicDBObject("url", sourceUrl.trim())
						.append("ad_class", "")
						.append("status", "0")
						.append("query_time", 1)
						.append("create_date", today)
						.append("update_date", today);
				dBCollection.insert(documents);
				
			}
		}
		
		dmpDataBean.setCategory(category);
		dmpDataBean.setCategorySource(categorySource);
		dmpDataBean.setClass24hUrlClassify(class24hUrlClassify);
		
		return dmpDataBean;
	}
	
	

	public DBObject queryClassUrl(DB mongoOperations,String url) throws Exception {
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("url",url));
		andQuery.put("$and", obj);
		DBObject dbObject =  dBCollection.findOne(andQuery);
		return dbObject;
	}
	
	public void updateClassUrl(DB mongoOperations,String url,DBObject dbObject) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todayStr = sdf.format(today);
		String updateDateStr = sdf.format(dbObject.get("update_date"));
		if ( (!todayStr.equals(updateDateStr)) ){
			Date date = new Date();
			dbObject.put("update_date", date);

		    DBObject olddbObject = new BasicDBObject();
		    olddbObject.put("url", sourceUrl.trim());
		    dBCollection.update(olddbObject, dbObject);
		}
		if ( (Integer.parseInt( dbObject.get("query_time").toString()) <2000) ){
//			Update querytime = new Update();
//			querytime.inc( "query_time" , 1 );
//			mongoOperations.updateFirst(new Query(Criteria.where( "url" ).is(sourceUrl.trim())), querytime, "class_url");
			
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.append("$inc", new BasicDBObject().append("query_time", 1));
			DBObject filter = new BasicDBObject(); 
			filter.put("url", sourceUrl.trim());
			dBCollection.update(filter,newDocument);
		}
	}
	
}