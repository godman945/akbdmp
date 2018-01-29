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

import com.pchome.akbdmp.api.data.enumeration.ClassCountMongoDBEnum;
import com.pchome.hadoopdmp.data.mongo.pojo.ClassUrlMongoBean;
import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.mapreduce.job.categorylog.CategoryLogMapper;

@SuppressWarnings({ "unchecked"})
public class AdRutenLog extends ACategoryLogData {

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
		
		Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
		Matcher m = p.matcher(sourceUrl);
		if(!m.find() ) {
			return null;
 		} 
		
		ClassUrlMongoBean classUrlMongoBean = null;
		Query query = new Query(Criteria.where("url").is(sourceUrl.trim()));
		classUrlMongoBean = mongoOperations.findOne(query, ClassUrlMongoBean.class);
		
		if(classUrlMongoBean != null){
			//爬蟲
			if(classUrlMongoBean.getStatus().equals("0")){
				adClass = crawlerGetAdclass(sourceUrl);
				if(StringUtils.isNotBlank(adClass)){
					Date date = new Date();
					classUrlMongoBean.setAd_class(adClass);
					classUrlMongoBean.setStatus("1");
					classUrlMongoBean.setUpdate_date(date);
					mongoOperations.save(classUrlMongoBean);
					behaviorClassify = "Y";
				}
			}
			
			//有ad_class
			if(classUrlMongoBean.getStatus().equals("1")){
				adClass = classUrlMongoBean.getAd_class();
				behaviorClassify = "Y";
			}
		}else {
			adClass = crawlerGetAdclass(sourceUrl);
			Date date = new Date();
			ClassUrlMongoBean classUrlMongoBeanCreate = new ClassUrlMongoBean();
			classUrlMongoBeanCreate.setAd_class(adClass.matches("\\d{16}") ?  adClass : "");
			classUrlMongoBeanCreate.setStatus(adClass.matches("\\d{16}") ? "1" : "0");
			classUrlMongoBeanCreate.setUrl(sourceUrl); 
			classUrlMongoBeanCreate.setCreate_date(date);
			classUrlMongoBeanCreate.setUpdate_date(date);
			mongoOperations.save(classUrlMongoBeanCreate);
		}
		
		
		//爬蟲沒有adclass
		if(!adClass.matches("\\d{16}")){
	    	return null;
	    }

	    //取個資
	    if((StringUtils.isNotBlank(memid)) && (!memid.equals("null")) ) {
			Query queryUserInfo = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
			UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(queryUserInfo, UserDetailMongoBean.class);
			String sex = "";
			String age = "";
			if(userDetailMongoBean != null){
				sex = (String)userDetailMongoBean.getUser_info().get("sex");
				age = (String)userDetailMongoBean.getUser_info().get("age");
				categoryLogBean.setPersonalInfoClassify("Y");
			}else{
				categoryLogBean.setPersonalInfoClassify("N");
			}
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ruten");
			categoryLogBean.setType("memid");
			categoryLogBean.setBehaviorClassify(behaviorClassify);
			categoryLogBean.setSex(StringUtils.isNotBlank(sex) ? sex : "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(age) ? age : "null");
			return categoryLogBean;
		}else if(StringUtils.isNotBlank(uuid) && (!uuid.equals("null"))){
//			APersonalInfo aPersonalInfo = PersonalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.UUID);
//			Map<String, Object> uuidMap = aPersonalInfo.getMap();
//			uuidMap.put("adClass", adClass); 
//			uuidMap.put("ClsfyCraspMap", CategoryLogMapper.clsfyCraspMap);
//			Map<String, Object> userInfo = (Map<String, Object>) aPersonalInfo.personalData(uuidMap);
			Query queryUserInfo = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(uuid));
			UserDetailMongoBean userDetailMongoBean =  mongoOperations.findOne(queryUserInfo, UserDetailMongoBean.class);
			String sex = "";
			String age = "";
			if(userDetailMongoBean != null){
				sex = (String)userDetailMongoBean.getUser_info().get("sex");
				age = (String)userDetailMongoBean.getUser_info().get("age");
				categoryLogBean.setPersonalInfoClassify("Y");
			}else{
				categoryLogBean.setPersonalInfoClassify("N");
			}
			categoryLogBean.setSex(StringUtils.isNotBlank(sex) ? sex : "null");
			categoryLogBean.setAge(StringUtils.isNotBlank(age) ? age : "null");
			categoryLogBean.setAdClass(adClass);
			categoryLogBean.setMemid(values[1]);
			categoryLogBean.setUuid(values[2]);
			categoryLogBean.setSource("ruten");
			categoryLogBean.setType("uuid");
			categoryLogBean.setBehaviorClassify(behaviorClassify);
			return categoryLogBean;
		}

		return null;
	}
	
	public String crawlerGetAdclass(String sourceUrl) throws Exception {
		StringBuffer url = new StringBuffer();
		String urlCated = "";// ad_class
		
		// url transform
		Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
		Matcher m = p.matcher(sourceUrl.toString());
		if (m.find()) {
			url.append("http://m.ruten.com.tw/goods/show.php?g=");
			url.append(m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", ""));
		} else {
			return null;
		}

//		Thread.sleep(500); 

		Document doc =  Jsoup.parse( new URL(url.toString()) , 10000 );

		Elements breadcrumbE = doc.body().select("ul[class=rt-breadcrumb-list]");

		String breadcrumb = breadcrumbE.size() > 0 ? breadcrumbE.get(0).text() : "nothing";

		// adult
		if (breadcrumb.equals("nothing")) {
			p = Pattern
					.compile(".+(http|https):\\\\/\\\\/member.ruten.com.tw\\\\/user\\\\/mlogin.php\\?refer=.+");
			m = p.matcher(doc.toString());
			if (m.find()) {
				urlCated = "0025000000000000";
			} else {
				return urlCated;
			}
			return urlCated;
		}

		//如果ad_class不是16位數字，去分類表比對
		if (!urlCated.matches("\\d{16}")) {
			StringBuffer tmpBuf = new StringBuffer().append(">").append(breadcrumb.replaceAll(" ", ">"));
			breadcrumb = tmpBuf.toString();

			String[] catedLvl = breadcrumb.replaceAll(" ", "").replaceAll("\t", "").replaceAll("@", "").trim()
					.split(">");

			ArrayList<Map<String, String>> list = CategoryLogMapper.categoryList;
			List<String> urlCatedList = CateMatch((catedLvl.length > 4 ? 4 : (catedLvl.length - 1)), catedLvl, list);
			urlCated = (urlCatedList.size() > 0 ? urlCatedList.get(0) : "unclassed");
			list=null;
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