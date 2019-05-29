package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import com.pchome.soft.util.MysqlUtil;
import com.pchome.soft.util.UAgentInfo;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class PaclLogConverCountReducer2 extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("PaclLogConverCountReducer2");
	private MysqlUtil mysqlUtil = null;
	private StringBuffer insertSqlStr = new StringBuffer();
	private StringBuffer querySqlStr = new StringBuffer();
	private static Date date = new Date();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
	private static List<JSONObject> dataCkList = new ArrayList<JSONObject>();
	//比對用ck物件
	private static List<JSONObject> comparisonDataList = new ArrayList<JSONObject>();
	private static List<JSONObject> dataPvList = new ArrayList<JSONObject>();
	private JSONObject paclJsonInfo = new JSONObject();
	//pacl整理後的log資訊
	private static List<JSONObject> paclJsonInfoList = new ArrayList<JSONObject>();
	private static Map<String,JSONObject> saveDBMap = new HashMap<String,JSONObject>();
	private static Map<String,String> actionPfpCodeMergeMap = new HashMap<String,String>();
	final static SimpleDateFormat sdfFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static boolean flagKdcl = false;
	private static boolean flagPacl = false;
	private static Long differenceDay = null;
	private static String kdclDate = "";
	private static Iterator<JSONObject> iterator = null;
	private static JSONObject iteratorJson = null;
	private static String rangrDate = null;
	private static String jobDate = null;
	private static int logMergeCount = 0;
	private static JSONObject logJson = new JSONObject();
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			jobDate = context.getConfiguration().get("job.date");
			log.info(" jobDate:"+jobDate);
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(context.getConfiguration().get("spring.profiles.active"));
			
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
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}
	
	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			log.info("-----------1");
			String key = mapperKey.toString();
			dataCkList.clear();
			dataPvList.clear();
			paclJsonInfo.clear();
			paclJsonInfoList.clear();
			comparisonDataList.clear();
			iteratorJson = null;
			iterator = null;
			differenceDay = null;
			flagKdcl = false;
			flagPacl = false;
			logJson.clear();
			if(StringUtils.isBlank(key)) {
				log.info("1>>>>>>>>>null");
				return;
			}
			for (Text text : mapperValue) {
				String value = text.toString();
				logJson = (JSONObject) jsonParser.parse(value);
				if(logJson.getAsString("fileName").contains("kdcl") || logJson.getAsString("fileName").contains("kwstg")){
					flagKdcl = true;
					String kdclType = logJson.getAsString("kdclType");
					if(kdclType.equals("ck")){
						dataCkList.add(logJson);
					}else if(kdclType.equals("pv")){
						dataPvList.add(logJson);
					}
				}
				if(logJson.getAsString("fileName").contains("part-r-")){
					flagPacl = true;
					paclJsonInfoList.add(logJson);
				}
			}
			
			if(flagKdcl && flagPacl){
//				logMergeCount = logMergeCount + 1;
//				log.info("key:"+key+" flagKdcl:"+flagKdcl+" flagPacl:"+flagPacl);
//				log.info(">>>>>>>>>paclJsonInfoList:"+paclJsonInfoList);
				for (JSONObject paclJson : paclJsonInfoList) {
					this.paclJsonInfo = paclJson;
					comparisonDataList.clear();
					for (JSONObject ckJson : dataCkList) {
						JSONObject comparisonJson = ((net.minidev.json.JSONObject) jsonParser.parse(ckJson.toString()));
						//根據廣告活動查詢對應的轉換代碼
						String actionSeq = comparisonJson.getAsString("actionSeq");
						if(StringUtils.isBlank((actionSeq))){
							continue;
						}
						String pfpCode = null;
						if(StringUtils.isBlank(actionPfpCodeMergeMap.get(actionSeq))){
							pfpCode = findPfpCodeAdactionMerge(actionSeq);
						}else{
							pfpCode = actionPfpCodeMergeMap.get(actionSeq);
						}
						if(StringUtils.isNotBlank(pfpCode)){
							comparisonJson.put("pfp_code", pfpCode);
							comparisonDataList.add(comparisonJson);
						}
					}
					
					//刪除不在追蹤時間內資料與活動綁定轉換代碼不一致資料
					processOutOfRangeDay(comparisonDataList,"ck");
					//排序時間
					sortKdclDataList(comparisonDataList);
					//存入寫入DB map
					processSaveDBInfo(comparisonDataList,"ck",key);
					comparisonDataList.clear();

					for (JSONObject pvJson : dataPvList) {
						JSONObject comparisonJson = ((net.minidev.json.JSONObject) jsonParser.parse(pvJson.toString()));
						//根據廣告活動查詢對應的轉換代碼
						String actionSeq = comparisonJson.getAsString("actionSeq");
						if(StringUtils.isBlank((actionSeq))){
							continue;
						}
						String pfpCode = null;
						if(StringUtils.isBlank(actionPfpCodeMergeMap.get(actionSeq))){
							pfpCode = findPfpCodeAdactionMerge(actionSeq);
						}else{
							pfpCode = actionPfpCodeMergeMap.get(actionSeq);
						}
						if(StringUtils.isNotBlank(pfpCode)){
							comparisonJson.put("pfp_code", pfpCode);
							comparisonDataList.add(comparisonJson);
						}
					}
					
					processOutOfRangeDay(comparisonDataList,"pv");
					sortKdclDataList(comparisonDataList);
					processSaveDBInfo(comparisonDataList,"pv",key);
				}
			}
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	
	private String findPfpCodeAdactionMerge(String actionSeq) throws Exception{
		querySqlStr.setLength(0);
		querySqlStr.append(" SELECT code_id FROM pfp_code_adaction_merge where 1=1 and code_type = 'C' and ad_action_seq = '").append(actionSeq).append("'");
		ResultSet resultSet = mysqlUtil.query(querySqlStr.toString());
		String code = null;
		while(resultSet.next()){
			code = resultSet.getString("code_id");
		}
		actionPfpCodeMergeMap.put(actionSeq, code);
		return code;
	}
	
	private void processOutOfRangeDay(List<JSONObject> data,String type) throws Exception{
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
//			log.info(">>>>>>>>>data ck list:"+data);
		}else if(type.equals("pv")){
			rangrDate = impRangeDate;
//			log.info(">>>>>>>>>data pv list:"+data);
		}
		iterator = data.iterator();
		while (iterator.hasNext()) {
			iteratorJson = (JSONObject)iterator.next();
			iteratorJson.put("clickRangeDate", clickRangeDate);
			iteratorJson.put("impRangeDate", impRangeDate);
			iteratorJson.put("convertPriceCount", convertPriceCount);
			iteratorJson.put("convertPrice", convertPrice);
			iteratorJson.put("convertBelong", convertBelong);
			iteratorJson.put("convertSeq", convertSeq);
			iteratorJson.put("convertNumType", convertNumType);
			iteratorJson.put("convertCount", convertCount);
			kdclDate = iteratorJson.getAsString("kdclDate");
			
			differenceDay = (long) (sdf.parse(jobDate).getTime() - sdf.parse(kdclDate).getTime()) / (1000 * 60 * 60 *24);
			
//			if(type.equals("pv")){
//				log.info(">>>>>>>>>iteratorJson:"+iteratorJson);
//				log.info(">>>>>>>>>paclJsonInfo:"+this.paclJsonInfo);
//				log.info(">>>>>>>>>differenceDay:"+differenceDay);
//				log.info(">>>>>>>>>rangrDate:"+rangrDate);
//				log.info(">>>>>>>>>pfp_code:"+iteratorJson.getAsString("pfp_code"));
//				log.info(">>>>>>>>>convertSeq:"+convertSeq);
//			}
			
			if(differenceDay > Long.valueOf(rangrDate)){
				iterator.remove();
				continue;
			}
			
			if(!iteratorJson.getAsString("pfp_code").equals(convertSeq)){
				iterator.remove();
				continue;
			}
		}
		
	}
	
	private List<JSONObject> sortKdclDataList(List<JSONObject> data){
		Collections.sort(comparisonDataList, new Comparator<JSONObject>() {
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
	
	private void processSaveDBInfo(List<JSONObject> data,String type,String uuid) throws Exception{
		if(data.size() > 0){
			//1:最終 2:最初
			String convertBelong = ((JSONObject)data.get(0)).getAsString("convertBelong");
			if(convertBelong.equals("1")){
				JSONObject saveJson = new JSONObject();
				saveJson = data.get(0);
				saveDBMap.put(uuid+"<PCHOME>"+type.toUpperCase()+"<PCHOME>"+saveJson.getAsString("convertSeq"), saveJson);
			}else if(convertBelong.equals("2")){
				JSONObject saveJson = new JSONObject();
				saveJson = data.get(data.size() - 1);
				saveDBMap.put(uuid+"<PCHOME>"+type.toUpperCase()+"<PCHOME>"+saveJson.getAsString("convertSeq"), saveJson);
			}
		}
	}
	
	private String getOS(UAgentInfo uAgentInfo){
		if (uAgentInfo.detectAndroid()) {
			return "Android";
        }
        else if (uAgentInfo.detectIphoneOrIpod()) {
        	return "IOS";
        }
        else if (uAgentInfo.detectWindowsMobile()) {
        	return "Windows";
        }
        else if (uAgentInfo.detectWindows()) {
        	return "Windows";
        }
		return "";
	}
	
	private String getTimeCode(int hour){
        if ((hour >= 0) && (hour <= 3)) {
            return "A";
        }
        else if ((hour >= 4) && (hour <= 7)) {
        	return "B";
        }
        else if ((hour >= 8) && (hour <= 11)) {
        	return "C";
        }
        else if ((hour >= 12) && (hour <= 15)) {
        	return "D";
        }
        else if ((hour >= 16) && (hour <= 19)) {
        	return "E";
        }
        else if ((hour >= 20) && (hour <= 23)) {
        	return "F";
        }
        return "";
	}
	
	private String getAgeCode(int age){
		if ((age >= 1) && (age <= 17)) {
            return "A";
        }
        else if ((age >= 18) && (age <= 24)) {
        	return "B";
        }
        else if ((age >= 25) && (age <= 34)) {
        	return "C";
        }
        else if ((age >= 35) && (age <= 44)) {
        	return "D";
        }
        else if ((age >= 45) && (age <= 54)) {
        	return "E";
        }
        else if ((age >= 55) && (age <= 64)) {
        	return "F";
        }
        else if ((age >= 65) && (age <= 74)) {
        	return "G";
        }
        else if (age >= 75) {
        	return "H";
        }
		return "I";
	}
	
	public void cleanup(Context context) {
		try {
			log.info("cleanup logMergeCount:"+logMergeCount);
			
			
			int count = 0;
			int totalSize = saveDBMap.size();
			log.info("cleanup saveDBMap size:"+totalSize);
			log.info("saveDBMap:"+saveDBMap);
			PreparedStatement preparedStmt = mysqlUtil.getConnect().prepareStatement(insertSqlStr.toString());
			Date date = new Date();
			for (Entry<String ,JSONObject> data : saveDBMap.entrySet()) {
				//寫入mysql
				count = count + 1;
				String uuid = data.getKey().split("<PCHOME>")[0];
				String type = data.getKey().split("<PCHOME>")[1];
				JSONObject json = data.getValue();
				 // device
		        UAgentInfo uAgentInfo = new UAgentInfo(json.getAsString("userAgent"), null);
		        String device = uAgentInfo.detectSmartphone() ? "mobile" : "PC";
		        String os = getOS(uAgentInfo);
		        
		        String timeCode = "";
		        if((Pattern.compile("^[0-9]+$").matcher(json.getAsString("keclTime")).find())){
		        	timeCode = getTimeCode(Integer.parseInt(json.getAsString("keclTime")));
		        }
		        String ageCode = "";
		        if((Pattern.compile("^[0-9]+$").matcher(json.getAsString("age")).find())){
		        	ageCode = getAgeCode(Integer.parseInt(json.getAsString("age")));
		        }
		        
		        
				preparedStmt.setString(1, uuid);
				preparedStmt.setString(2, jobDate);
				preparedStmt.setString(3, json.getAsString("convertSeq"));
				preparedStmt.setString(4, type);
				preparedStmt.setString(5, json.getAsString("convertNumType"));
				preparedStmt.setString(6, json.getAsString("convertBelong"));
				preparedStmt.setString(7, json.getAsString("kdclDate"));
				preparedStmt.setInt(8, Integer.parseInt(json.getAsString("convertCount")));
				preparedStmt.setInt(9,(int)Double.parseDouble(json.getAsString(("convertPriceCount"))));
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
				preparedStmt.setString(22,ageCode );
				preparedStmt.setString(23,timeCode );
				preparedStmt.setString(24,json.getAsString("tproId") );
				preparedStmt.setString(25,json.getAsString("categoryCode") );
				preparedStmt.setString(26,json.getAsString("*****") );
				preparedStmt.setString(27,json.getAsString("referer") );
				preparedStmt.setString(28,json.getAsString("styleId") );
				preparedStmt.setString(29,device);
				preparedStmt.setString(30,os);
				preparedStmt.setString(31,json.getAsString("*****"));
				preparedStmt.setString(32,json.getAsString("*****"));
				preparedStmt.setString(33, sdfFormat.format(date));
				preparedStmt.setString(34, sdfFormat.format(date));
				preparedStmt.addBatch();
				if(count % 5000 == 0){
					preparedStmt.executeBatch();
					mysqlUtil.getConnect().commit();
				}else if(count == totalSize){
					preparedStmt.executeBatch();
					mysqlUtil.getConnect().commit();
					preparedStmt.close();
				}
			}
			log.info(">>>>>>>>mysql insert finish");
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
}
