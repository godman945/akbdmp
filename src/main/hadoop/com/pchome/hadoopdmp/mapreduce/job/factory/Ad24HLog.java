package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;


public class Ad24HLog extends ACategoryLogData {

	public Object processCategory(DmpLogBean dmpDataBean, MongoOperations mongoOperations) throws Exception {
		
		String memid = dmpDataBean.getMemid();
		String uuid = dmpDataBean.getUuid();
		String sourceUrl = dmpDataBean.getUrl();
		String adClass = "";
		String class24hUrl = "";
		
		if (StringUtils.isBlank(sourceUrl)) {
			dmpDataBean.setAdClass(adClass);
			dmpDataBean.setUrl("");
			dmpDataBean.setClass24hUrl("N");
			dmpDataBean.setSource("24h");
			return dmpDataBean;
		}
		
		List<CategoryCodeBean> list = DmpLogMapper.category24hBeanList;
		for (CategoryCodeBean categoryBean : list) {
			if(sourceUrl.indexOf(categoryBean.getEnglishCode()) != -1){
				adClass = categoryBean.getNumberCode();
				class24hUrl = "Y";
				break;
			}
		}
		
		if (StringUtils.isBlank(adClass)){
			ClassUrlMongoBean classUrlMongoBean = null;
			Query query = new Query(Criteria.where("url").is(sourceUrl.trim()));
			classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
			
			if(classUrlMongoBean != null){
				if(classUrlMongoBean.getStatus().equals("0")){
					// url 存在 status = 0  , mongo update_date 更新(一天一次) query_time+1 如大於 2000 不再加 
					adClass ="";
					class24hUrl = "N";
					
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
					//url 存在 status = 1 取分類代號回傳 mongodn update_date 更新(一天一次) behaviorClassify = "Y"
					adClass = classUrlMongoBean.getAd_class();
					class24hUrl = "Y"; 
					
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
				// url 不存在  ,寫入 mongo url代號 status=0 
				adClass ="";
				class24hUrl = "N";
				
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
		
		
		dmpDataBean.setAdClass(adClass);
		dmpDataBean.setClass24hUrl(class24hUrl);
		dmpDataBean.setSource("24h");
		
		return dmpDataBean;
	}
	
}