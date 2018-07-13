package com.pchome.hadoopdmp.mapreduce.job.component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper.combinedValue;
import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;
import com.pchome.soft.util.HttpUtil;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

public class ThirdAdClassComponent {
	
	Log log = LogFactory.getLog("thirdAdClassComponent");
	
	private DBCollection dBCollection;
	
	// 處理第3分類元件
	public DmpLogBean processThirdAdclassInfo(DmpLogBean dmpDataBean ,DB mongoOperations) throws Exception {
		try{
			log.info(">>>>>> processThirdAdclassInfo");
			
			this.dBCollection= mongoOperations.getCollection("class_url_third_adclass");
			
			String category = dmpDataBean.getCategory();				//取得第1、2層分類
			String categorySource = dmpDataBean.getCategorySource();	//取得第1、2層分類來源
			String url = dmpDataBean.getUrl();
			
			if (!category.matches("\\d{16}")) {
				return dmpDataBean;
			}
			
			if ( (!StringUtils.equals("24h", categorySource)) &&  (!StringUtils.equals("ruten", categorySource)) ){
				return dmpDataBean;
			}
			
			//check url是否符合 24h、ruten的 pattern
			Pattern p = null;
			if ( StringUtils.equals("24h", categorySource) ){
				p = Pattern.compile("https://24h.pchome.com.tw/prod/");
			}else if( StringUtils.equals("ruten", categorySource) ){
				p = Pattern.compile("http://goods.ruten.com.tw/item/show+\\?\\d+");
			}
			Matcher m = p.matcher(url.toString());
			if (!m.find()) {
				log.info(">>>>>> 不符合 url:------- " + url.toString());
				return dmpDataBean;
			}
			
			log.info(">>>>>> category>>> "+category);
			log.info(">>>>>> categorySource>>> "+categorySource);
			log.info(">>>>>> url>>> "+url);
			
			
			
			//url 作 md5 編碼當 key
			//撈 url key 撈 mongodb 看有沒有第三層資料
			if ( StringUtils.equals("24h", categorySource) ){
				url = url.split("\\?")[0];
			}
			
			String urlToMd5 = getMD5(url);
			
			log.info(">>>>>> urlToMd5 : "+urlToMd5);
			
			DBObject dbObject = queryClassUrlThirdAdclass(urlToMd5);
			if (dbObject != null) {
				//如果mongo已有第3層資料，直接將array塞進dmpDataBean的Prod_class_info
				ArrayList<String> mongoThirdAdclassList = new ArrayList<String>();  
				mongoThirdAdclassList = (ArrayList<String>) dbObject.get("prod_class_info");
				
				dmpDataBean.setProdClassInfo(mongoThirdAdclassList);
				
				log.info(">>>>>>dbObject != null : "+urlToMd5);
				
			}else{
				
				log.info(">>>>>>dbObject== null >>>>>>>>>>>>>>>"+urlToMd5);
				
				//第3層資料沒有在mongo中，打爬蟲get標題
				String prodTitle = "";
				JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
				StringBuffer adCrawlerResult = HttpUtil.getInstance().doGet("http://pysvr.mypchome.com.tw/product/?url="+url);
				JSONObject apiJsonObject = (net.minidev.json.JSONObject)jsonParser.parse(adCrawlerResult.toString());
				JSONArray apiJsonAry = (JSONArray) apiJsonObject.get("products");
				for (Object object : apiJsonAry) {
					JSONObject infoJson = (JSONObject) object;
					prodTitle = infoJson.getAsString("title");
					log.info(">>>>>> url title : "+prodTitle);
				}
				
				log.info(">>>>>>dbObject== null >>>>>>url>>>>>>>>>"+url);
				//mark
	//			prodTitle = "【良匠工具】電鑽鐵板剪(適用於切割鋁板,銅板,不鏽鋼板...)";
				//mark
				
				//比對title是否有命中第3分類對照表(ThirdAdClassTable.txt)
				ArrayList<String> matchProdList = new ArrayList<String>();
				for (String string : DmpLogMapper.prodFileList) {
					if ( prodTitle.indexOf(string) > -1){
						matchProdList.add(string);
					}
				}
				//將有命中之第3分類中文傳至reduceer依序新增
				dmpDataBean.setProdClassInfo(matchProdList);
				dmpDataBean.setUrlToMd5(urlToMd5);
			}
			
			return dmpDataBean;
			
	} catch (Throwable e) {
		log.error("processThirdAdclassInfo error>>>>>> " +e);
	}
		return dmpDataBean;
	}
	

	
	public DBObject queryClassUrlThirdAdclass(String urlToMd5) throws Exception {
		BasicDBObject andQuery = new BasicDBObject();
		List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
		obj.add(new BasicDBObject("_id", urlToMd5));
		andQuery.put("$and", obj);
		DBObject dbObject =  dBCollection.findOne(andQuery);
		return dbObject;
	}
	
	
	/**
	 * 字串md5加密
	 *
	 * @param str
	 * @return
	 */
	public static String getMD5(String str) {
        MessageDigest md = null;
	    try {
	    	md = MessageDigest.getInstance("MD5");
	        md.update(str.getBytes());
	    } catch (Exception e) {
	    	System.out.println(e);
	    }
		return  new BigInteger(1, md.digest()).toString(16);
	}
}