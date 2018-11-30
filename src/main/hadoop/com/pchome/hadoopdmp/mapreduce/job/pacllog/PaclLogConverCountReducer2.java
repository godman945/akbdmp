package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import com.pchome.soft.util.MysqlUtil;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class PaclLogConverCountReducer2 extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("PaclLogConverCountReducer2");
	private Text keyOut = new Text();
	private Text valueOut = new Text();

	private MysqlUtil mysqlUtil = null;
	private StringBuffer convertWriteInfo = new StringBuffer();
	private StringBuffer sql = new StringBuffer();
	private static Date date = new Date();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
	private static List<JSONObject> dataCkList = new ArrayList<JSONObject>();
	private static List<JSONObject> dataPvList = new ArrayList<JSONObject>();
	private static Map<String,JSONObject> saveDBMap = new HashMap<String,JSONObject>();
	final static SimpleDateFormat sdfFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static boolean flagKdcl = false;
	private static boolean flagPacl = false;
	private static Long differenceDay = null;
	private static String clickRangeDate = "";
	private static String impRangeDate = "";
	private static String convertPriceCount = "";
	private static String convertPrice = "";
	private static String convertBelong = "";
	private static String convertSeq = "";
	private static String kdclDate = "";
	private static Iterator<JSONObject> iterator = null;
	private static JSONObject iteratorJson = null;
	
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "keyword";
			String password =  "K1y0nLine";
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(url, user, password);
			
			log.info("mysqlUtil:"+mysqlUtil.getInstance());
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}
	
	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			String key = mapperKey.toString();
			dataCkList.clear();
			dataPvList.clear();
			iteratorJson = null;
			iterator = null;
			differenceDay = null;
			convertSeq = "";
			clickRangeDate = "";
			impRangeDate = "";
			convertPriceCount = "";
			convertPrice = "";
			kdclDate = "";
			//1:最終 2:最初
			convertBelong = "";
			flagKdcl = false;
			flagPacl = false;
			if(key.equals("2f59086e290c6a4a513834ba16f563e6")){
				for (Text text : mapperValue) {
					String value = text.toString();
					log.info(">>>"+value);
					JSONObject logJson = (JSONObject) jsonParser.parse(value);
					log.info("fileName:"+logJson.getAsString("fileName"));
					log.info("kdcl:"+logJson.getAsString("fileName").contains("kdcl"));
					log.info("pacl:"+logJson.getAsString("fileName").contains("part-r-00000"));
					if(logJson.getAsString("fileName").contains("kdcl")){
						flagKdcl = true;
						String kdclType = logJson.getAsString("kdclType");
						if(kdclType.equals("ck")){
							dataCkList.add(logJson);
						}else if(kdclType.equals("pv")){
							dataPvList.add(logJson);
						}
					}
					if(logJson.getAsString("fileName").contains("part-r-00000")){
						flagPacl = true;
						clickRangeDate = logJson.getAsString("clickRangeDate");
						impRangeDate = logJson.getAsString("impRangeDate");
						convertPriceCount = logJson.getAsString("convertPriceCount");
						convertPrice = logJson.getAsString("convertPrice");
						convertBelong = logJson.getAsString("convertBelong");
						convertSeq = logJson.getAsString("convertSeq");
					}
				}
			}
			if(flagKdcl && flagPacl){
				log.info("##>>>>>>key:"+key);
				processOutOfRangeDay(dataCkList,"ck");
				//排序時間
				sortKdclDataList(dataCkList);
				log.info("after sort kdclDate ck:"+ dataCkList);
				//存入寫入DB map
				processSaveDBInfo(dataCkList,"ck",key);
				
				
				processOutOfRangeDay(dataPvList,"pv");
				sortKdclDataList(dataPvList);
				log.info("after sort kdclDate pv:"+ dataPvList);
				processSaveDBInfo(dataPvList,"pv",key);
				
				
				
//				iterator = dataCkList.iterator();
//				while (iterator.hasNext()) {
//					iteratorJson = (JSONObject)iterator.next();
//					kdclDate = iteratorJson.getAsString("kdclDate");
//					differenceDay = (long) (date.getTime() - sdf.parse(kdclDate).getTime()) / (1000 * 60 * 60 *24);
//					log.info("kdclDate ck:"+kdclDate+" flag:"+(differenceDay <= Long.valueOf(clickRangeDate)) + " range:"+differenceDay);
//					if(differenceDay > Long.valueOf(clickRangeDate)){
//						iterator.remove();
//					}
//				}
				
				
//				if(dataCkList.size() > 0){
//					if(convertBelong.equals("1")){
//						JSONObject saveJson = new JSONObject();
//						saveJson = dataCkList.get(dataCkList.size() - 1);
//						saveDBMap.put(key+"_ck", saveJson);
//						log.info("final data:"+dataCkList.get(dataCkList.size() - 1));
//					}
//					if(convertBelong.equals("2")){
//						JSONObject saveJson = new JSONObject();
//						saveJson = dataCkList.get(0);
//						saveDBMap.put(key+"_ck", saveJson);
//						log.info("final data:"+dataCkList.get(0));
//					}
//				}
			}
			
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	
	private void processSaveDBInfo(List<JSONObject> data,String type,String uuid) throws Exception{
		if(data.size() > 0){
			
			if(convertBelong.equals("1")){
				JSONObject saveJson = new JSONObject();
				saveJson = data.get(data.size() - 1);
				saveDBMap.put(uuid+"<PCHOME_"+type.toUpperCase()+">", saveJson);
				log.info("final data:"+data.get(data.size() - 1));
			}
			if(convertBelong.equals("2")){
				JSONObject saveJson = new JSONObject();
				saveJson = data.get(0);
				saveDBMap.put(uuid+"<PCHOME_"+type.toUpperCase()+">", saveJson);
				log.info("final data:"+data.get(0));
			}
		}
	}
	
	private void processOutOfRangeDay(List<JSONObject> data,String type) throws Exception{
		log.info("processSaveDBInfo type:"+type);
		String rangrDate = null;
		if(type.equals("ck")){
			rangrDate = clickRangeDate;
		}else if(type.equals("pv")){
			rangrDate = impRangeDate;
		}
		iterator = data.iterator();
		while (iterator.hasNext()) {
			iteratorJson = (JSONObject)iterator.next();
			kdclDate = iteratorJson.getAsString("kdclDate");
			differenceDay = (long) (date.getTime() - sdf.parse(kdclDate).getTime()) / (1000 * 60 * 60 *24);
			log.info("kdclDate:"+kdclDate+" flag:"+(differenceDay <= Long.valueOf(rangrDate)) + " range:"+differenceDay);
			if(differenceDay > Long.valueOf(rangrDate)){
				iterator.remove();
			}
		}
	}
	
	private List<JSONObject> sortKdclDataList(List<JSONObject> data){
		Collections.sort(dataCkList, new Comparator<JSONObject>() {
			public int compare(JSONObject o1, JSONObject o2) {
				try {
					return sdf.parse(o2.getAsString("kdclSourceDate")).compareTo(sdf.parse(o1.getAsString("kdclSourceDate")));
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
			}
		});
		return data;
	}
	
	public void cleanup(Context context) {
		try {
			
			log.info("cleanup:"+saveDBMap);
			
			
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	
	
}
