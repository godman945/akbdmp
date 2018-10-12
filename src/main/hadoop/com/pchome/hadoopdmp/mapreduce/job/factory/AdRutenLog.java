package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;

@SuppressWarnings({ "unchecked"})
public class AdRutenLog extends ACategoryLogData {
	Log log = LogFactory.getLog("AdRutenLog");
	private DBCollection dBCollection;
	
	public Object processCategory(DmpLogBean dmpDataBean, DB mongoOperations) throws Exception {
		this.dBCollection= mongoOperations.getCollection("class_url");
		
		dmpDataBean.setSource("kdcl");
		
		String sourceUrl = dmpDataBean.getUrl();
		String category = "null";
		String categorySource = "null";
		String classRutenUrlClassify = "null" ;
		
		if (StringUtils.isBlank(sourceUrl)) {
			dmpDataBean.setUrl("null");
			dmpDataBean.setCategory("null");
			dmpDataBean.setCategorySource("null");
			dmpDataBean.setClassRutenUrlClassify("N");
			return dmpDataBean;
		}
		
		//查詢url
		DBObject dbObject =queryClassUrl(sourceUrl.trim()) ;
		
		if(dbObject != null){
			if(dbObject.get("status").equals("0")){
				category = "null";
				categorySource = "null";
				classRutenUrlClassify = "N"; 
				// url 存在 status = 0 跳過回傳空值 , mongo update_date 更新(一天一次) mongo,query_time+1 如大於 2000 不再加  classRutenUrl = "N"
				updateClassUrlUpdateDate(sourceUrl.trim(),dbObject) ;
				updateClassUrlQueryTime( sourceUrl.trim(),dbObject) ;
			}else if( (dbObject.get("status").equals("1")) && (StringUtils.isNotBlank(dbObject.get("ad_class").toString())) ){
				category = dbObject.get("ad_class").toString();
				categorySource = "ruten";
				classRutenUrlClassify = "Y"; 
				//url 存在 status = 1 取分類代號回傳 mongodn update_date 更新(一天一次) classRutenUrl = "Y";
				updateClassUrlUpdateDate(sourceUrl.trim(),dbObject) ;
			}
		} else {
			try {
				// url 不存在
				StringBuffer transformUrl = new StringBuffer();
				Pattern p = Pattern.compile("(http|https)://goods.ruten.com.tw/item/\\S+\\?\\d+");
				Matcher m = p.matcher(sourceUrl.toString());

				if (m.find()) {
					// url是Ruten商品頁，爬蟲撈麵包屑(http與https)
					transformUrl.append("http://m.ruten.com.tw/goods/show.php?g=");
					transformUrl.append(m.group().replaceAll("(http|https)://goods.ruten.com.tw/item/\\S+\\?", ""));
					// Thread.sleep(500);
					Document doc = Jsoup.parse(new URL(transformUrl.toString()), 10000);
					Elements breadcrumbE = doc.body().select("table[class=goods-list]");
					String breadcrumbResult = "";

					if (breadcrumbE.size() > 0) {
						for (int i = 0; i < breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").size(); i++) {
							if (i != (breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").size() - 1)) {
								breadcrumbResult = breadcrumbResult
										+ breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").get(i).text()
										+ ">";
							} else {
								breadcrumbResult = breadcrumbResult
										+ breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").get(i).text();
							}

						}

						// 比對爬蟲回來的分類，由最底層分類開始比對Ruten分類表
						String[] breadcrumbAry = breadcrumbResult.split(">");
						List<CategoryRutenCodeBean> categoryRutenList = DmpLogMapper.categoryRutenBeanList;
						boolean hasCategory = false;

						for (int i = breadcrumbAry.length - 1; i >= 0; i--) {
							if (hasCategory) {
								break;
							}
							for (CategoryRutenCodeBean categoryRutenBean : categoryRutenList) {
								if (categoryRutenBean.getChineseDesc().trim().equals(breadcrumbAry[i].trim())) {
									category = categoryRutenBean.getNumberCode();
									hasCategory = true;
									break;
								}
							}
						}

						if (StringUtils.isNotBlank(category)) {
							// 爬蟲有比對到Ruten分類
							categorySource = "ruten";
							classRutenUrlClassify = "Y";
							//新增url
							insertClassUrl(sourceUrl.trim(),"1",category,breadcrumbResult,"",0) ;
						} else {
							// 麵包屑沒有比對到Ruten分類
							category = "null";
							categorySource = "null";
							classRutenUrlClassify = "N";
							insertClassUrl(sourceUrl.trim(),"0","",breadcrumbResult,"爬不到麵包屑的訊息",1) ;
						}
					} else {
						// 沒有麵包屑
						category = "null";
						categorySource = "null";
						classRutenUrlClassify = "N";
						insertClassUrl(sourceUrl.trim(),"0","","","沒有麵包屑的訊息",1) ;
					}
				} else {
					// url不是Ruten商品頁，寫入mongo
					category = "null";
					categorySource = "null";
					classRutenUrlClassify = "N";
					insertClassUrl(sourceUrl.trim(),"0","","","url不符合Ruten商品頁",1) ;
				}

			} catch (Exception e) {
				category = "null";
				categorySource = "null";
				classRutenUrlClassify = "N";
				insertClassUrl(sourceUrl.trim(),"0","","","",1) ;
				log.error(">>>>>>"+ e.getMessage());
			}
		}

		dmpDataBean.setCategory(category);
		dmpDataBean.setCategorySource(categorySource);
		dmpDataBean.setClassRutenUrlClassify(classRutenUrlClassify);
		
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
	
	public void insertClassUrl(String url,String status,String adClass,String rutenBread,String errMsg,int queryTime) throws Exception {
		Date today = new Date();
		DBObject documents = new BasicDBObject("url", url)
				.append("status", status)
				.append("ad_class", adClass)
				.append("ruten_bread", rutenBread)
				.append("err_msg", errMsg)
				.append("query_time", queryTime)
				.append("create_date", today)
				.append("update_date", today);
		dBCollection.insert(documents);
	}
}