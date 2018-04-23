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
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper;


public class Ad24HLog extends ACategoryLogData {

	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = values[1];
		String uuid = values[2];
		String sourceUrl = values[4];
		String adClass = "";
		String behaviorClassify = "N";
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null"))) {
			return null;
		}

		if (StringUtils.isBlank(sourceUrl)) {
			return null;
		}
		
//		Pattern p = Pattern.compile("(http|https)://24h.pchome.com.tw/(store|region)/([a-zA-Z0-9]+)([&|\\?|\\.]\\S*)?");
//		Matcher m = p.matcher(sourceUrl);
//		if (!m.find()) {
//			return null;
//		}
		
		List<CategoryCodeBean> list = CategoryLogMapper.category24hBeanList;
		for (CategoryCodeBean categoryBean : list) {
			if(sourceUrl.indexOf(categoryBean.getEnglishCode()) != -1){
				adClass = categoryBean.getNumberCode();
				behaviorClassify = "Y";
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
					behaviorClassify = "N";
					
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
					behaviorClassify = "Y"; 
					
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
				behaviorClassify = "N";
				
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
		categoryLogBean.setMemid(values[1]);
		categoryLogBean.setUuid(values[2]);
		categoryLogBean.setSource("24h");
//		categoryLogBean.setType("uuid");
		categoryLogBean.setBehaviorClassify(behaviorClassify);
		return categoryLogBean;
	}
	
}