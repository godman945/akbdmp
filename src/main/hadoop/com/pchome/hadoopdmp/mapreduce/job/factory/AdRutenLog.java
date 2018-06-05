package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.net.URL;
import java.text.SimpleDateFormat;
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
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.DB;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;

@SuppressWarnings({ "unchecked"})
public class AdRutenLog extends ACategoryLogData {
	
	Log log = LogFactory.getLog("AdRutenLog");

	public Object processCategory(DmpLogBean dmpDataBean, DB mongoOperations) throws Exception {
		
		dmpDataBean.setSource("kdcl");
		
//		String sourceUrl = dmpDataBean.getUrl();
//		String category = "";
//		String categorySource = "";
//		String classRutenUrlClassify = "" ;
//		
//		if (StringUtils.isBlank(sourceUrl)) {
//			dmpDataBean.setUrl("null");
//			dmpDataBean.setCategory("null");
//			dmpDataBean.setCategorySource("null");
//			dmpDataBean.setClassRutenUrlClassify("null");
//			return dmpDataBean;
//		}
//		
//		ClassUrlMongoBean classUrlMongoBean = null;
//		Query query = new Query(Criteria.where("url").is(sourceUrl.trim()));
//		classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
//		
//		if(classUrlMongoBean != null){
//			
//			if(classUrlMongoBean.getStatus().equals("0")){
//				// url 存在 status = 0 跳過回傳空值 , mongo update_date 更新(一天一次) mongo,query_time+1 如大於 2000 不再加  classRutenUrl = "N"
//				category = "null";
//				categorySource = "null";
//				classRutenUrlClassify = "N"; 
//
//				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//				Date today = new Date();
//				String todayStr = sdf.format(today);
//				
//				Date updateDate = classUrlMongoBean.getUpdate_date();
//				String updateDateStr = sdf.format(updateDate);
//				
//				if ( (!todayStr.equals(updateDateStr)) ){
//					Date date = new Date();
//					classUrlMongoBean.setUpdate_date(date);					
//					mongoOperations.save(classUrlMongoBean);
//				}
//				
//				if ( (classUrlMongoBean.getQuery_time()<2000) ){
//					Update querytime = new Update();
//					querytime.inc( "query_time" , 1 );
//					mongoOperations.updateFirst(new Query(Criteria.where( "url" ).is(sourceUrl.trim())), querytime, "class_url");
//				}
//				
//			}else if( (classUrlMongoBean.getStatus().equals("1")) && (StringUtils.isNotBlank(classUrlMongoBean.getAd_class())) ){
//				//url 存在 status = 1 取分類代號回傳 mongodn update_date 更新(一天一次) classRutenUrl = "Y";
//				category = classUrlMongoBean.getAd_class();
//				categorySource = "ruten";
//				classRutenUrlClassify = "Y"; 
//				
//				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//				Date today = new Date();
//				String todayStr = sdf.format(today);
//				
//				Date updateDate = classUrlMongoBean.getUpdate_date();
//				String updateDateStr = sdf.format(updateDate);
//				
//				if ( (!todayStr.equals(updateDateStr)) ){
//					classUrlMongoBean.setUpdate_date(today);
//					mongoOperations.save(classUrlMongoBean);
//				}
//			}
//			
//		} else {
//			try {
//				// url 不存在
//				StringBuffer transformUrl = new StringBuffer();
//				Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
//				Matcher m = p.matcher(sourceUrl.toString());
//
//				if (m.find()) {
//					// url是Ruten商品頁，爬蟲撈麵包屑
//					transformUrl.append("http://m.ruten.com.tw/goods/show.php?g=");
//					transformUrl.append(m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", ""));
//					// Thread.sleep(500);
//					Document doc = Jsoup.parse(new URL(transformUrl.toString()), 10000);
//					Elements breadcrumbE = doc.body().select("table[class=goods-list]");
//					String breadcrumbResult = "";
//
//					if (breadcrumbE.size() > 0) {
//						for (int i = 0; i < breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").size(); i++) {
//							if (i != (breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").size() - 1)) {
//								breadcrumbResult = breadcrumbResult
//										+ breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").get(i).text()
//										+ ">";
//							} else {
//								breadcrumbResult = breadcrumbResult
//										+ breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").get(i).text();
//							}
//
//						}
//
//						// 比對爬蟲回來的分類，由最底層分類開始比對Ruten分類表
//						String[] breadcrumbAry = breadcrumbResult.split(">");
//						List<CategoryRutenCodeBean> categoryRutenList = DmpLogMapper.categoryRutenBeanList;
//						boolean hasCategory = false;
//
//						for (int i = breadcrumbAry.length - 1; i >= 0; i--) {
//							if (hasCategory) {
//								break;
//							}
//							for (CategoryRutenCodeBean categoryRutenBean : categoryRutenList) {
//								if (categoryRutenBean.getChineseDesc().trim().equals(breadcrumbAry[i].trim())) {
//									category = categoryRutenBean.getNumberCode();
//									hasCategory = true;
//									break;
//								}
//							}
//						}
//
//						if (StringUtils.isNotBlank(category)) {
//							// 爬蟲有比對到Ruten分類
//							categorySource = "ruten";
//							classRutenUrlClassify = "Y";
//
//							Date date = new Date();
//							ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//							classUrlMongoBeanCreate.setUrl(sourceUrl.trim());
//							classUrlMongoBeanCreate.setStatus("1");
//							classUrlMongoBeanCreate.setAd_class(category);
//							classUrlMongoBeanCreate.setRuten_bread(breadcrumbResult);
//							classUrlMongoBeanCreate.setCreate_date(date);
//							classUrlMongoBeanCreate.setUpdate_date(date);
//							mongoOperations.save(classUrlMongoBeanCreate);
//						} else {
//							// 麵包屑沒有比對到Ruten分類
//							category = "null";
//							categorySource = "null";
//							classRutenUrlClassify = "N";
//
//							Date date = new Date();
//							ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//							classUrlMongoBeanCreate.setUrl(sourceUrl.trim());
//							classUrlMongoBeanCreate.setStatus("0");
//							classUrlMongoBeanCreate.setAd_class("");
//							classUrlMongoBeanCreate.setRuten_bread(breadcrumbResult);
//							classUrlMongoBeanCreate.setErr_msg("爬不到麵包屑的訊息");
//							classUrlMongoBeanCreate.setQuery_time(1);
//							classUrlMongoBeanCreate.setCreate_date(date);
//							classUrlMongoBeanCreate.setUpdate_date(date);
//							mongoOperations.save(classUrlMongoBeanCreate);
//						}
//					} else {
//						// 沒有麵包屑
//						category = "null";
//						categorySource = "null";
//						classRutenUrlClassify = "null";
//
//						Date date = new Date();
//						ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//						classUrlMongoBeanCreate.setUrl(sourceUrl.trim());
//						classUrlMongoBeanCreate.setStatus("0");
//						classUrlMongoBeanCreate.setAd_class("");
//						classUrlMongoBeanCreate.setQuery_time(1);
//						classUrlMongoBeanCreate.setCreate_date(date);
//						classUrlMongoBeanCreate.setUpdate_date(date);
//						mongoOperations.save(classUrlMongoBeanCreate);
//					}
//				} else {
//					// url不是Ruten商品頁，寫入mongo
//					category = "null";
//					categorySource = "null";
//					classRutenUrlClassify = "null";
//
//					Date date = new Date();
//					ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//					classUrlMongoBeanCreate.setUrl(sourceUrl.trim());
//					classUrlMongoBeanCreate.setStatus("0");
//					classUrlMongoBeanCreate.setAd_class("");
//					classUrlMongoBeanCreate.setQuery_time(1);
//					classUrlMongoBeanCreate.setCreate_date(date);
//					classUrlMongoBeanCreate.setUpdate_date(date);
//					mongoOperations.save(classUrlMongoBeanCreate);
//				}
//
//			} catch (Exception e) {
//				category = "null";
//				categorySource = "null";
//				classRutenUrlClassify = "null";
//
//				Date date = new Date();
//				ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
//				classUrlMongoBeanCreate.setUrl(sourceUrl.trim());
//				classUrlMongoBeanCreate.setStatus("0");
//				classUrlMongoBeanCreate.setAd_class("");
//				classUrlMongoBeanCreate.setQuery_time(1);
//				classUrlMongoBeanCreate.setCreate_date(date);
//				classUrlMongoBeanCreate.setUpdate_date(date);
//				mongoOperations.save(classUrlMongoBeanCreate);
//				
//				log.error(">>>>>>"+ e.getMessage());
//			}
//		}
//
//		dmpDataBean.setCategory(category);
//		dmpDataBean.setCategorySource(categorySource);
//		dmpDataBean.setClassRutenUrlClassify(classRutenUrlClassify);
		
		return dmpDataBean;
	}
}