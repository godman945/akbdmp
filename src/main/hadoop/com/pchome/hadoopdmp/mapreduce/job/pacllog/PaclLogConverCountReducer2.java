package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.PreparedStatement;
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
import java.util.Map.Entry;

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
	private StringBuffer insertSqlStr = new StringBuffer();
	private StringBuffer sql = new StringBuffer();
	private static Date date = new Date();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
	private static List<JSONObject> dataCkList = new ArrayList<JSONObject>();
	private static List<JSONObject> dataPvList = new ArrayList<JSONObject>();
	private static JSONObject paclJsonInfo = new JSONObject();
	
	
	private static Map<String,JSONObject> saveDBMap = new HashMap<String,JSONObject>();
	final static SimpleDateFormat sdfFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static boolean flagKdcl = false;
	private static boolean flagPacl = false;
	private static Long differenceDay = null;
	private static String kdclDate = "";
	private static Iterator<JSONObject> iterator = null;
	private static JSONObject iteratorJson = null;
	private static String rangrDate = null;
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "keyword";
			String password =  "K1y0nLine";
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(url, user, password);
			
			insertSqlStr.append(" INSERT INTO `pfp_code_convert_trans`  ");
			insertSqlStr.append("(uuid,");
			insertSqlStr.append("convert_date,");
			insertSqlStr.append("convert_seq,");
			insertSqlStr.append("convert_trigger_type,");
			insertSqlStr.append("convert_num_type,");
			insertSqlStr.append("convert_belong, ");
			insertSqlStr.append("convert_belong_date,");
			insertSqlStr.append("convert_count,");
			insertSqlStr.append("convert_price,");
			insertSqlStr.append("ad_seq,");
			insertSqlStr.append("ad_group_seq,");
			insertSqlStr.append("ad_action_seq,");
			insertSqlStr.append("ad_type,");
			insertSqlStr.append("ad_pvclk_date,");
			insertSqlStr.append("ad_pvclk_time,");
			insertSqlStr.append("customer_info_id,");
			insertSqlStr.append("pfbx_customer_info_id,");
			insertSqlStr.append("pfbx_position_id,");
			insertSqlStr.append("pfd_customer_info_id,");
			insertSqlStr.append("pay_type,");
			insertSqlStr.append("sex,");
			insertSqlStr.append("age_code,");
			insertSqlStr.append("time_code,");
			insertSqlStr.append("template_ad_seq,");
			insertSqlStr.append("ad_pvclk_website_classify,");
			insertSqlStr.append("ad_pvclk_audience_classify,");
			insertSqlStr.append("ad_url,");
			insertSqlStr.append("style_no,");
			insertSqlStr.append("ad_pvclk_device,");
			insertSqlStr.append("ad_pvclk_os,");
			insertSqlStr.append("ad_pvclk_brand,");
			insertSqlStr.append("ad_pvclk_area,");
			insertSqlStr.append("update_date,");
			insertSqlStr.append("create_date) ");
			insertSqlStr.append(" VALUES(  ");
			insertSqlStr.append(" 	?,?,?,?,?,?,?,?,?,?, ");
			insertSqlStr.append(" 	?,?,?,?,?,?,?,?,?,?, ");
			insertSqlStr.append(" 	?,?,?,?,?,?,?,?,?,?, ");
			insertSqlStr.append(" 	?,?,?,? )");
			
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
			paclJsonInfo.clear();
			iteratorJson = null;
			iterator = null;
			differenceDay = null;
			//1:最終 2:最初
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
						paclJsonInfo = logJson;
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
			}
			
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	
	private void processSaveDBInfo(List<JSONObject> data,String type,String uuid) throws Exception{
		if(data.size() > 0){
			String convertBelong = ((JSONObject)data.get(0)).getAsString("convertBelong");
			if(convertBelong.equals("1")){
				JSONObject saveJson = new JSONObject();
				saveJson = data.get(data.size() - 1);
				saveDBMap.put(uuid+"<PCHOME>"+type.toUpperCase(), saveJson);
				log.info("final data:"+data.get(data.size() - 1));
			}
			if(convertBelong.equals("2")){
				JSONObject saveJson = new JSONObject();
				saveJson = data.get(0);
				saveDBMap.put(uuid+"<PCHOME>"+type.toUpperCase(), saveJson);
				log.info("final data:"+data.get(0));
			}
		}
	}
	
	private void processOutOfRangeDay(List<JSONObject> data,String type) throws Exception{
		log.info("processSaveDBInfo type:"+type);
		String clickRangeDate = paclJsonInfo.getAsString("clickRangeDate");
		String impRangeDate = paclJsonInfo.getAsString("impRangeDate");
		String convertPriceCount = paclJsonInfo.getAsString("convertPriceCount");
		String convertPrice = paclJsonInfo.getAsString("convertPrice");
		String convertBelong = paclJsonInfo.getAsString("convertBelong");
		String convertSeq = paclJsonInfo.getAsString("convertSeq");
		String convertNumType = paclJsonInfo.getAsString("convertNumType");
		String convertCount = paclJsonInfo.getAsString("convertCount");
		rangrDate = null;
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
			}else{
				iteratorJson.put("clickRangeDate", clickRangeDate);
				iteratorJson.put("impRangeDate", impRangeDate);
				iteratorJson.put("convertPriceCount", convertPriceCount);
				iteratorJson.put("convertPrice", convertPrice);
				iteratorJson.put("convertBelong", convertBelong);
				iteratorJson.put("convertSeq", convertSeq);
				iteratorJson.put("convertNumType", convertNumType);
				iteratorJson.put("convertCount", convertCount);
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
			for (Entry<String ,JSONObject> data : saveDBMap.entrySet()) {
				
				
//				String clickRangeDate = paclJsonInfo.getAsString("clickRangeDate");
//				String impRangeDate = paclJsonInfo.getAsString("impRangeDate");
//				String convertPriceCount = paclJsonInfo.getAsString("convertPriceCount");
//				String convertPrice = paclJsonInfo.getAsString("convertPrice");
//				String convertBelong = paclJsonInfo.getAsString("convertBelong");
//				String convertSeq = paclJsonInfo.getAsString("convertSeq");
//				String convertNumType = paclJsonInfo.getAsString("convertNumType");
//				String convertNumType = convertNumType
				String type = data.getKey().split("<PCHOME>")[1];
				String uuid = data.getKey().split("<PCHOME>")[0];
				JSONObject json = data.getValue();
				PreparedStatement preparedStmt = mysqlUtil.getConnect().prepareStatement(insertSqlStr.toString());
				preparedStmt.setString(1, uuid);
				preparedStmt.setString(2, sdf.format(date));
				preparedStmt.setString(3, json.getAsString("convertSeq"));
				preparedStmt.setString(4, type);
				preparedStmt.setString(5, json.getAsString("convertNumType"));
				preparedStmt.setString(6, json.getAsString("convertBelong"));
				preparedStmt.setString(7, json.getAsString("kdclDate"));
				preparedStmt.setInt(8, Integer.parseInt(json.getAsString("convertCount")));
				preparedStmt.setInt(9,Integer.parseInt(json.getAsString("convertPriceCount")));
				preparedStmt.setString(10,json.getAsString("adSeq") );
				preparedStmt.setString(11,json.getAsString("groupSeq") );
				preparedStmt.setString(12,json.getAsString("actionSeq"));
				preparedStmt.setString(13,json.getAsString("adType") );
				preparedStmt.setString(14,json.getAsString("kdclDate") );
				preparedStmt.setString(15,json.getAsString("keclTime") );
				preparedStmt.setString(16,json.getAsString("pfpCustomerInfoId") );
				preparedStmt.setString(17,json.getAsString("pfbxCustomerInfoId") );
				preparedStmt.setString(18,json.getAsString("pfbxPositionId") );
				preparedStmt.setString(19,json.getAsString("pfdCustomerInfoId") );
				preparedStmt.setString(20,json.getAsString("payType") );
				preparedStmt.setString(21,json.getAsString("sex") );
				preparedStmt.setString(22,json.getAsString("ageCode") );
				preparedStmt.setString(23,json.getAsString("*****") );
				preparedStmt.setString(24,json.getAsString("*****") );
				preparedStmt.setString(25,json.getAsString("*****") );
				preparedStmt.setString(26,json.getAsString("*****") );
				preparedStmt.setString(27,json.getAsString("referer") );
				preparedStmt.setString(28,json.getAsString("styleId") );
				preparedStmt.setString(29,json.getAsString("*****") );
				preparedStmt.setString(30,json.getAsString("*****") );
				preparedStmt.setString(31,json.getAsString("*****"));
				preparedStmt.setString(32,json.getAsString("*****"));
				preparedStmt.setDate(33, java.sql.Date.valueOf(sdf.format(date)));
				preparedStmt.setDate(34,java.sql.Date.valueOf(sdf.format(date)));
//				PreparedStatement preparedStmt = connect.prepareStatement(sql);
//				preparedStmt.setString (1, "Barney");
//				preparedStmt.setString (2, "Rubble");
//				preparedStmt.setDate   (3, startDate);
//				preparedStmt.setBoolean(4, false);
//				preparedStmt.setInt    (5, 5000);
				
				mysqlUtil.insert(preparedStmt);
			}
			
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			sql.setLength(0);
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	
	
}
