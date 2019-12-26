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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;

@SuppressWarnings({ "unchecked"})
public class AdRutenLog extends ACategoryLogData {
	Log log = LogFactory.getLog("AdRutenLog");
	private static DBObject filter = new BasicDBObject(); 
	private static DBCollection dBCollection;
	private static Date today = new Date();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String sourceUrl = "";
	private static String classRutenUrlClassify = "";
	private static String category = "";
	private static String categorySource = "";
	private static BasicDBObject newDocument = new BasicDBObject();
	private static DBObject dbObject = null;
	private static BasicDBObject andQuery = new BasicDBObject();
	private static List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
	private static BasicDBObject basicDBObject = new BasicDBObject();
	private static BasicDBObject query = new BasicDBObject();
	private static Pattern p = Pattern.compile("(http|https)://goods.ruten.com.tw/item/\\S+\\?\\d+");
	private static Matcher matcher = null;
	private static StringBuffer transformUrl = new StringBuffer();
	private static String breadcrumbResult = "";
	private static String[] breadcrumbAry = null;
	private static boolean hasCategory = false;
	private static Document doc;
	private static Elements breadcrumbE;
	private static BasicDBObject intModifier = new BasicDBObject();
	public Object processCategory(net.minidev.json.JSONObject dmpJSon, DBCollection dbCollectionUrl) throws Exception {
		System.out.println("*********** ruten 1");
		
		transformUrl.setLength(0);
		category = "";
		categorySource = "";
		classRutenUrlClassify = "" ;
		this.dBCollection = dbCollectionUrl;
		sourceUrl = dmpJSon.getAsString("referer");
		
		System.out.println("*********** ruten 2");
		
		if (StringUtils.isBlank(sourceUrl)) {
//			dmpJSon.put("class_ruten_url_classify", "N");
			dmpJSon.put("classify", "N");
			dmpJSon.put("behavior", "ruten");
			return dmpJSon;
		}else {
			System.out.println("*********** ruten 3");
			
			//查詢url
			dbObject = queryClassUrl(sourceUrl.trim());
			if(dbObject != null){
				if(dbObject.get("status").equals("0")){
					System.out.println("*********** ruten 4");
					
					classRutenUrlClassify = "N"; 
					// url 存在 status = 0 跳過回傳空值 , mongo update_date 更新(一天一次) mongo,query_time+1 如大於 2000 不再加  classRutenUrl = "N"
//					log.info("dbObject:"+dbObject);
					updateClassUrlUpdateDate(sourceUrl.trim(),dbObject);
					updateClassUrlQueryTime(sourceUrl.trim(),dbObject);
					
					System.out.println("*********** ruten 5");
					
				}else if((dbObject.get("status").equals("1")) && (StringUtils.isNotBlank(dbObject.get("ad_class").toString()))){
					System.out.println("*********** ruten 6");
					
					category = dbObject.get("ad_class").toString();
					categorySource = "ruten";
					classRutenUrlClassify = "Y"; 
					//url 存在 status = 1 取分類代號回傳 mongodn update_date 更新(一天一次) classRutenUrl = "Y";
					updateClassUrlUpdateDate(sourceUrl.trim(),dbObject);
					
					System.out.println("*********** ruten 7");
				}
			} else {
				try {
					// url 不存在
					matcher = p.matcher(sourceUrl.toString());
					if (matcher.find()) {
						// url是Ruten商品頁，爬蟲撈麵包屑(http與https)
						transformUrl.append("http://m.ruten.com.tw/goods/show.php?g=");
						transformUrl.append(matcher.group().replaceAll("(http|https)://goods.ruten.com.tw/item/\\S+\\?", ""));
						// Thread.sleep(500);
						doc = Jsoup.parse(new URL(transformUrl.toString()), 10000);
						breadcrumbE = doc.body().select("table[class=goods-list]");
						breadcrumbResult = "";
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
							breadcrumbAry = breadcrumbResult.split(">");
							hasCategory = false;
							for (int i = breadcrumbAry.length - 1; i >= 0; i--) {
								if (hasCategory) {
									break;
								}
								for (CategoryRutenCodeBean categoryRutenBean : DmpLogMapper.categoryRutenBeanList) {
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
								category = "";
								categorySource = "";
								classRutenUrlClassify = "N";
								insertClassUrl(sourceUrl.trim(),"0","",breadcrumbResult,"爬不到麵包屑的訊息",1) ;
							}
						} else {
							// 沒有麵包屑
							category = "";
							categorySource = "";
							classRutenUrlClassify = "N";
							insertClassUrl(sourceUrl.trim(),"0","","","沒有麵包屑的訊息",1) ;
						}
					} else {
						// url不是Ruten商品頁，寫入mongo
						category = "";
						categorySource = "";
						classRutenUrlClassify = "N";
						insertClassUrl(sourceUrl.trim(),"0","","","url不符合Ruten商品頁",1) ;
					}
				} catch (Exception e) {
					category = "";
					categorySource = "";
					classRutenUrlClassify = "N";
					insertClassUrl(sourceUrl.trim(),"0","","","",1) ;
					log.error(">>>>>>"+ e.getMessage());
				}
			}
		}
		dmpJSon.put("classify", classRutenUrlClassify);
		dmpJSon.put("behavior", categorySource);
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
		if ( (!todayStr.equals(updateDateStr)) ){
			Date date = new Date();
			dbObject.put("update_date", date);
		    DBObject olddbObject = new BasicDBObject();
		    olddbObject.put("url", url);
		    dBCollection.update(olddbObject, dbObject);
		}
	}
	
	public void updateClassUrlQueryTime(String url,DBObject dbObject) throws Exception {
//		log.info(">>>>>>>dbObject.get(query_time):"+dbObject.get("query_time"));
		query.clear();
		intModifier.clear();
		query.put("_id", dbObject.get("_id"));
		intModifier = intModifier.append("$inc", new BasicDBObject().append("query_time", 1));
		this.dBCollection.update(query, intModifier, false, false, WriteConcern.SAFE);
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