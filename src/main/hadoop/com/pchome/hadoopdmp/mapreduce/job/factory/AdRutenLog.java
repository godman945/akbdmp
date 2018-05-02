package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper;

@SuppressWarnings({ "unchecked"})
public class AdRutenLog extends ACategoryLogData {

	public Object processCategory(CategoryLogBean categoryRawDataBean, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = categoryRawDataBean.getMemid();
		String uuid = categoryRawDataBean.getUuid();
		String sourceUrl = categoryRawDataBean.getUrl();
		String adClass = "";
		String classRutenUrl = "" ;
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null"))) {
			return null;
		}

		if (StringUtils.isBlank(sourceUrl)) {
			return null;
		}
		
		ClassUrlMongoBean classUrlMongoBean = null;
		Query query = new Query(Criteria.where("url").is(sourceUrl.trim()));
		classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
		
		if(classUrlMongoBean != null){
			
			if(classUrlMongoBean.getStatus().equals("0")){
				// url 存在 status = 0 跳過回傳空值 , mongo update_date 更新(一天一次) mongo,query_time+1 如大於 2000 不再加  classRutenUrl = "N"
				classRutenUrl = "N"; 

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date today = new Date();
				String todayStr = sdf.format(today);
				
				Date updateDate = classUrlMongoBean.getUpdate_date();
				String updateDateStr = sdf.format(updateDate);
				
				if ( (!todayStr.equals(updateDateStr)) ){
					Date date = new Date();
					classUrlMongoBean.setUpdate_date(date);					
					mongoOperations.save(classUrlMongoBean);
				}
				
				if ( (classUrlMongoBean.getQuery_time()<2000) ){
					Update querytime = new Update();
					querytime.inc( "query_time" , 1 );
					mongoOperations.updateFirst(new Query(Criteria.where( "url" ).is(sourceUrl.trim())), querytime, "class_url");
				}
				
			}else if( (classUrlMongoBean.getStatus().equals("1")) && (!classUrlMongoBean.getAd_class().equals("")) ){
				//url 存在 status = 1 取分類代號回傳 mongodn update_date 更新(一天一次) classRutenUrl = "Y";
				adClass = classUrlMongoBean.getAd_class();
				classRutenUrl = "Y"; 
				
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date today = new Date();
				String todayStr = sdf.format(today);
				
				Date updateDate = classUrlMongoBean.getUpdate_date();
				String updateDateStr = sdf.format(updateDate);
				
				if ( (!todayStr.equals(updateDateStr)) ){
					classUrlMongoBean.setUpdate_date(today);
					mongoOperations.save(classUrlMongoBean);
				}
			}
			
		}else{
			//url 不存在
			StringBuffer transformUrl = new StringBuffer();
			Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
			Matcher m = p.matcher(sourceUrl.toString());
			
			if (m.find()) {
				//url是Ruten商品頁，爬蟲撈麵包屑
				transformUrl.append("http://m.ruten.com.tw/goods/show.php?g=");
				transformUrl.append(m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", ""));
//				Thread.sleep(500); 
				Document doc =  Jsoup.parse( new URL(transformUrl.toString()) , 10000 );
				Elements breadcrumbE = doc.body().select("table[class=goods-list]");
				String breadcrumbResult = "";
				
				if (breadcrumbE.size()>0){
					for (int i = 0; i < breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").size(); i++) {
						if (i != (breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").size() - 1)) {
							breadcrumbResult = breadcrumbResult + breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").get(i).text() + ">";
						} else {
							breadcrumbResult = breadcrumbResult + breadcrumbE.get(0).getElementsByClass("rt-breadcrumb-link").get(i).text();
						}
		
					}
					
					//比對爬蟲回來的分類，由最底層分類開始比對Ruten分類表
					String[] breadcrumbAry = breadcrumbResult.split(">");
					List<CategoryRutenCodeBean> categoryRutenList = CategoryLogMapper.categoryRutenBeanList;
					boolean hasCategory = false;
					
					for(int i = breadcrumbAry.length-1; i >=0; i--){
						if (hasCategory){
							break;
						}
						for (CategoryRutenCodeBean categoryRutenBean : categoryRutenList) {
							if (categoryRutenBean.getChineseDesc().trim().equals(breadcrumbAry[i].trim())){
								adClass = categoryRutenBean.getNumberCode();
								hasCategory = true;
								break;
							}
						}
					}
					
				
					if (StringUtils.isNotBlank(adClass)){
						//爬蟲有比對到Ruten分類
						classRutenUrl = "Y";
						
						Date date = new Date();
						ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
						classUrlMongoBeanCreate.setUrl(sourceUrl);
						classUrlMongoBeanCreate.setAd_class(adClass);
						classUrlMongoBeanCreate.setStatus("1");
						classUrlMongoBeanCreate.setCreate_date(date);
						classUrlMongoBeanCreate.setUpdate_date(date);
						mongoOperations.save(classUrlMongoBeanCreate);
					}else{
						//爬蟲沒有比對到Ruten分類
						classRutenUrl = "N";
						
						Date date = new Date();
						ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
						classUrlMongoBeanCreate.setUrl(sourceUrl);
						classUrlMongoBeanCreate.setAd_class("");
						classUrlMongoBeanCreate.setStatus("0");
						classUrlMongoBeanCreate.setQuery_time(1);
						classUrlMongoBeanCreate.setCreate_date(date);
						classUrlMongoBeanCreate.setUpdate_date(date);
						mongoOperations.save(classUrlMongoBeanCreate);
					}
				}
			} else {
				//url不是Ruten商品頁，寫入mongo
				Date date = new Date();
				ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
				classUrlMongoBeanCreate.setUrl(sourceUrl);
				classUrlMongoBeanCreate.setAd_class("");
				classUrlMongoBeanCreate.setStatus("0");
				classUrlMongoBeanCreate.setQuery_time(1);
				classUrlMongoBeanCreate.setCreate_date(date);
				classUrlMongoBeanCreate.setUpdate_date(date);
				mongoOperations.save(classUrlMongoBeanCreate);
			}
		}
		
		//最後如果adClass為空，不寫入Reducer
		if (StringUtils.isBlank(adClass)){
			return null;
		}
		
		
		categoryLogBean.setAdClass(adClass);
		categoryLogBean.setMemid(memid);
		categoryLogBean.setUuid(uuid);
		categoryLogBean.setSource("ruten");
		categoryLogBean.setClassRutenUrl(classRutenUrl);
		return categoryLogBean;
	}
}