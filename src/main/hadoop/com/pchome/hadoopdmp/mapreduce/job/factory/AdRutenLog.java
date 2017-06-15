package com.pchome.hadoopdmp.mapreduce.job.factory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.enumerate.PersonalInfoEnum;

public class AdRutenLog extends ACategoryLogData {

	public Object processCategory(String[] values, CategoryLogBean categoryLogBean,MongoOperations mongoOperations) throws Exception {
		
		String memid = values[1];
		String uuid = values[2];
		String sourceUrl = values[4];
		String adClass = "";
		
		
		if ((StringUtils.isBlank(memid) || memid.equals("null")) && (StringUtils.isBlank(uuid) || uuid.equals("null"))) {
			return null;
		}

		if (StringUtils.isBlank(sourceUrl)) {
			return null;
		}
		
//		String urlStr = values[4].toString();
//		StringBuffer url = new StringBuffer();

		
//		// url transform
//		Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
//		Matcher m = p.matcher(urlStr.toString());
//		if( m.find() ) {
//			url.append("http://m.ruten.com.tw/goods/show.php?g=");
//			url.append( m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", "") );
// 		} else {
// 			return null;
// 		}

		
		ClassUrlMongoBean classUrlMongoBean = null;
		Query query = new Query(Criteria.where("url").is(sourceUrl.trim()));
		classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
		
		if(classUrlMongoBean != null){
			//爬蟲
			if(classUrlMongoBean.getStatus().equals("0")){
				adClass = crawlerGetAdclass(categoryLogBean,sourceUrl);
				if(StringUtils.isNotBlank(adClass)){
					Date date = new Date();
					classUrlMongoBean.setAd_class(adClass);
					classUrlMongoBean.setStatus("1");
					classUrlMongoBean.setUpdate_dateDate(date);
					mongoOperations.save(classUrlMongoBean);
				}
			}
			
			//比對個資
			if(classUrlMongoBean.getStatus().equals("1")){
				adClass = classUrlMongoBean.getAd_class();
			}
		}
		
		if (classUrlMongoBean == null){
			adClass = crawlerGetAdclass(categoryLogBean,sourceUrl);
			Date date = new Date();
			ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
			classUrlMongoBeanCreate.setAd_class(adClass);
			classUrlMongoBeanCreate.setStatus(StringUtils.isBlank(adClass) ? "0" : "1");
			classUrlMongoBeanCreate.setUrl(sourceUrl); 
			classUrlMongoBeanCreate.setCreate_date(date);
			classUrlMongoBeanCreate.setUpdate_dateDate(date);
			mongoOperations.save(classUrlMongoBeanCreate);
		}
		
		
		
		
		//1.爬蟲沒有adclass
	    if (StringUtils.isBlank(adClass)) {
	    	return null;
	    }

	    //2取個資
	    if((StringUtils.isNotBlank(memid)) && (!memid.equals("null")) ) {
	    	categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ruten");
			categoryLogBean.setType("memid");
			return categoryLogBean;
		}else if(StringUtils.isNotBlank(uuid)){
			APersonalInfo aPersonalInfo = PersonalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.UUID);
			Map<String, Object> uuidMap = aPersonalInfo.getMap();
			uuidMap.put("adClass", adClass); 
			uuidMap.put("ClsfyCraspMap", categoryLogBean.getClsfyCraspMap());
			Map<String, Object> userInfo = (Map<String, Object>) aPersonalInfo.personalData(uuidMap);
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ruten");
			categoryLogBean.setSex(StringUtils.isNotBlank(userInfo.get("sex").toString()) ? userInfo.get("sex").toString(): "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(userInfo.get("age").toString()) ? userInfo.get("age").toString(): "null");
			categoryLogBean.setType("uuid");
			return categoryLogBean;
		}

		return null;
	}
	
	public String crawlerGetAdclass(CategoryLogBean categoryLogBean, String sourceUrl) throws Exception {
		StringBuffer url = new StringBuffer();
		
		// url transform
		Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
		Matcher m = p.matcher(sourceUrl.toString());
		if (m.find()) {
			url.append("http://m.ruten.com.tw/goods/show.php?g=");
			url.append(m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", ""));
		} else {
			// log.info("unrecognized url: " + value.toString());
			return null;
		}

		Thread.sleep(500); // test

		Document doc =  Jsoup.parse( new URL(url.toString()) , 10000 );

		Elements breadcrumbE = doc.body().select("ul[class=rt-breadcrumb-list]");

		String breadcrumb = breadcrumbE.size() > 0 ? breadcrumbE.get(0).text() : "nothing";
		// log.info("breadcrumb=" + breadcrumb);

		String urlCated = "";// ad_class
		// adult
		if (breadcrumb.equals("nothing")) {
			p = Pattern
					.compile(".+(http|https):\\\\/\\\\/member.ruten.com.tw\\\\/user\\\\/mlogin.php\\?refer=.+");
			m = p.matcher(doc.toString());
			if (m.find()) {
				urlCated = "0025000000000000";
				// context.write( new Text("0025000000000000") , value);
				// log.info("contextWrite: "+ "0025000000000000" + "," +
				// value.toString());
			} else {
				// context.write( new Text("unclassed") , value);
				// log.info("contextWrite: "+ "unclassed" + "," +
				// value.toString());
			}
			return urlCated;
		}

		if (!urlCated.matches("\\d{16}")) {
			StringBuffer tmpBuf = new StringBuffer().append(">").append(breadcrumb.replaceAll(" ", ">"));
			breadcrumb = tmpBuf.toString();
			// log.info("breadcrumb(af)=" + breadcrumb);

			String[] catedLvl = breadcrumb.replaceAll(" ", "").replaceAll("\t", "").replaceAll("@", "").trim()
					.split(">");

			ArrayList<Map<String, String>> list = categoryLogBean.getList();
			List<String> urlCatedList = CateMatch((catedLvl.length > 4 ? 4 : (catedLvl.length - 1)), catedLvl, list);
			urlCated = (urlCatedList.size() > 0 ? urlCatedList.get(0) : "unclassed");
		}

		urlCated = urlCated.matches("\\d{16}") ? urlCated : "";

		return urlCated;
	}
	
	
	public static List<String> CateMatch(int nowSearchLvl, String [] catedLvl ,ArrayList<Map<String, String>> list) {

		if( nowSearchLvl==0 ) {
			List<String> templist = new ArrayList<String>();
			templist.add("nothing");
			return templist;
		}

		String [] matchPatternTemp = catedLvl[nowSearchLvl].split("、");

		List <String> listSearchResult = likeStringSearch(list.get(nowSearchLvl-1), matchPatternTemp);

	    if( listSearchResult.size()==1 || nowSearchLvl==1 ) {
	    	return listSearchResult;
	    } else {
	    	listSearchResult = CateMatch(nowSearchLvl-1, catedLvl, list);
	    }

	    return listSearchResult;
	}
	
	public static List<String> likeStringSearch(Map<String, String> map, String[] matchPatternTemp) {
	    List<String> list = new ArrayList<String>();
	    Iterator it = map.entrySet().iterator();
	    int score;
	    int maxScore = 0;
	    while(it.hasNext()) {
	        Map.Entry<String, String> entry = (Map.Entry<String, String>)it.next();
	        score = 0;
	        for(int i=0;i<matchPatternTemp.length;i++) {
	        	if( entry.getValue().contains(matchPatternTemp[i]) )
	        		score++;
	        }
	        if( score>0 ) {
	        	if( score>maxScore )
	        		list.add(0, entry.getKey());
	        	else
	        		list.add(entry.getKey());
	        }

	    }
	    return list;
	}
}