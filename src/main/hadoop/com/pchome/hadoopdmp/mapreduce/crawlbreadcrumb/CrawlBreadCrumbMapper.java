package com.pchome.hadoopdmp.mapreduce.crawlbreadcrumb;

import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

@Component
public class CrawlBreadCrumbMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Log log = LogFactory.getLog(this.getClass());


	@Override
    public void map(LongWritable offset, Text value, Context context) {
		org.apache.log4j.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF);
		try {
			log.info("rutenURL:" + value.toString());	//debug

			String urlStr = value.toString();
			StringBuffer url = new StringBuffer();

			// url transform
			Pattern p = Pattern.compile("http://goods.ruten.com.tw/item/\\S+\\?\\d+");
    		Matcher m = p.matcher(urlStr.toString());
    		if( m.find() ) {
    			url.append("http://m.ruten.com.tw/goods/show.php?g=");
    			url.append( m.group().replaceAll("http://goods.ruten.com.tw/item/\\S+\\?", "") );
     		} else {
     			log.info("unrecognized url: " + value.toString());
     			return;
     		}

    		Thread.sleep(500);	//test

    		Document doc =  Jsoup.parse( new URL(url.toString()) , 10000 );

			Elements breadcrumbE = doc.body().select("ul[class=rt-breadcrumb-list]");

			String breadcrumb = breadcrumbE.size()>0 ? breadcrumbE.get(0).text() : "nothing" ;
			log.info("breadcrumb=" + breadcrumb);

			// adult
			if( breadcrumb.equals("nothing") ) {
				p = Pattern.compile(".+(http|https):\\\\/\\\\/member.ruten.com.tw\\\\/user\\\\/mlogin.php\\?refer=.+");
	    		m = p.matcher(doc.toString());
	    		if( m.find() ) {
	    			context.write( new Text("0025000000000000") , value);
	    			log.info("contextWrite: "+ "0025000000000000" + "," + value.toString());
	     		} else {
	     			context.write( new Text("unclassed") , value);
	     			log.info("contextWrite: "+ "unclassed" + "," + value.toString());
	     		}
	    		return;
			}


			StringBuffer tmpBuf = new StringBuffer()
					.append(">")
					.append(breadcrumb.replaceAll(" ", ">"));
			breadcrumb = tmpBuf.toString();
			//log.info("breadcrumb(af)=" + breadcrumb);

			// get csv file
			Configuration conf = context.getConfiguration();
	        org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);

	        Path cate_path = Paths.get(path[0].toString());
			Charset charset = Charset.forName("UTF-8");

		    int maxCateLvl = 4;
		    ArrayList<Map<String, String>> list = new ArrayList<Map<String,String>>();

		    for(int i=0;i<maxCateLvl;i++) {
		    	list.add( new HashMap<String,String>() );
		    }

			List<String> lines = Files.readAllLines(cate_path, charset);

			//將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
		    for (String line : lines) {
		    	String [] tmpStr = line.split(";");

		    	int lvl = Integer.parseInt( tmpStr[5].replaceAll("\"", "").trim() );
		    	if( lvl <= maxCateLvl ) {
		    		list.get(lvl-1).put( tmpStr[3].replaceAll("\"", "").trim() , tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim() );
		    	}

		    }


		    String [] catedLvl = breadcrumb.replaceAll(" ", "").replaceAll("\t", "") .replaceAll("@", "").trim().split(">");

		    List<String> urlCatedList = CateMatch( (catedLvl.length>4?4:(catedLvl.length-1)), catedLvl, list);
		    String urlCated = (urlCatedList.size()>0?urlCatedList.get(0):"unclassed");

		    log.info("contextWrite: "+ urlCated + "," + value.toString());
		    context.write( new Text(urlCated) , value);

		} catch (Exception e) {
			log.error(e);
		}



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
