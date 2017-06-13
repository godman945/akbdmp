package com.pchome.dmp.mapreduce.job.factory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.pchome.dmp.enumerate.PersonalInfoEnum;

public class Ad24HLog extends ACategoryLogData {

public Object processCategory(String[] values, Object obj,CategoryLogBean categoryLogBean) throws Exception {
		
		
//		log.info("rutenURL:" + value.toString());	//debug

		String urlStr = values[4].toString();
		StringBuffer url = new StringBuffer();

		// url transform
		Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
		Matcher m = p.matcher(urlStr.toString());
		if( m.find() ) {
			url.append("http://m.ruten.com.tw/goods/show.php?g=");
			url.append( m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", "") );
 		} else {
// 			log.info("unrecognized url: " + value.toString());
 			return null;
 		}

		Thread.sleep(500);	//test

		Document doc =  Jsoup.parse( new URL(url.toString()) , 10000 );

		Elements breadcrumbE = doc.body().select("ul[class=rt-breadcrumb-list]");

		String breadcrumb = breadcrumbE.size()>0 ? breadcrumbE.get(0).text() : "nothing" ;
//		log.info("breadcrumb=" + breadcrumb);

		// adult
		if( breadcrumb.equals("nothing") ) {
			p = Pattern.compile(".+(http|https):\\\\/\\\\/member.ruten.com.tw\\\\/user\\\\/mlogin.php\\?refer=.+");
    		m = p.matcher(doc.toString());
    		if( m.find() ) {
//    			context.write( new Text("0025000000000000") , value);
//    			log.info("contextWrite: "+ "0025000000000000" + "," + value.toString());
     		} else {
//     			context.write( new Text("unclassed") , value);
//     			log.info("contextWrite: "+ "unclassed" + "," + value.toString());
     		}
    		return null;
		}


		StringBuffer tmpBuf = new StringBuffer().append(">").append(breadcrumb.replaceAll(" ", ">"));
		breadcrumb = tmpBuf.toString();
		//log.info("breadcrumb(af)=" + breadcrumb);


	    String [] catedLvl = breadcrumb.replaceAll(" ", "").replaceAll("\t", "") .replaceAll("@", "").trim().split(">");

	    ArrayList<Map<String, String>> list = categoryLogBean.getList();
	    List<String> urlCatedList = CateMatch( (catedLvl.length>4?4:(catedLvl.length-1)), catedLvl, list);
	    String urlCated = (urlCatedList.size()>0?urlCatedList.get(0):"unclassed");

	    categoryLogBean.setAdClass(urlCated.matches("\\d{16}") ? urlCated : "");

	    //拿個資
//	    PersonalInfoFactory personalInfoFactory = (PersonalInfoFactory) obj;
//		APersonalInfo aPersonalInfo = personalInfoFactory.getAPersonalInfoFactory(PersonalInfoEnum.UUID);
//		Map<String,Object> map = aPersonalInfo.getMap();
//		map.put("ad_class", categoryLogBean.getAdClass());
//		map.put("list", list);
//		aPersonalInfo.personalData(map);
		
	    Map<String, com.pchome.dmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue> clsfyCraspMap = categoryLogBean.getClsfyCraspMap();
	    com.pchome.dmp.mapreduce.job.categorylog.CategoryLogMapper.combinedValue combineObj = clsfyCraspMap.get(urlCated);
		
	    categoryLogBean.setAdClass(StringUtils.isNotBlank(urlCated) ? urlCated : "null");
	    categoryLogBean.setAge(StringUtils.isNotBlank(combineObj.age) ? combineObj.age : "null");
	    categoryLogBean.setSex(StringUtils.isNotBlank(combineObj.gender) ? combineObj.gender : "null");
	    categoryLogBean.setUuid(StringUtils.isNotBlank(values[2]) ? values[2] : "null");
	    categoryLogBean.setMemid(StringUtils.isNotBlank(values[1]) ?  "null" : "null");
		return categoryLogBean;
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