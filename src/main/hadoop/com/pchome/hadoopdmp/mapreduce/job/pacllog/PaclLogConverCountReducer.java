package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.soft.util.MysqlUtil;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class PaclLogConverCountReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("PaclLogConverCountReducer");

	private Text keyOut = new Text();

	public static String record_date;

	public static Producer<String, String> producer = null;

	public RedisTemplate<String, Object> redisTemplate = null;

	public int count;

	public JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);

	public String redisFountKey;

	public Map<String, JSONObject> kafkaDmpMap = null;

	public Map<String, Integer> redisClassifyMap = null;
	
	public static Map<String, PcalConditionBean> convertConditionMap = new HashMap<String, PcalConditionBean>();

	private MysqlUtil mysqlUtil = null;
	
	private final static SimpleDateFormat sdfFormat =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static Date now =  new Date();
	
	
	private StringBuffer convertWriteInfo = new StringBuffer();
	
	private StringBuffer sql = new StringBuffer();
	
	private Map<String,Set<String>> convertResultMap = new HashMap<>();
	
	private Set<String> convertConditionSet = new HashSet<String>();	
	
	private static PcalConditionBean pcalConditionBean;
	
	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	
	private static List<JSONObject> paclLogList = new ArrayList<JSONObject>();
	
	private static String[] convertConditionArray = null;
	
	private static Integer userDefineConvertPrice = null;
	
	private static String convertSeq = null;
	
	private static String trackingSeq = null;
	
	private static Map<String,Map<String,String>> saveHbaseTrackingMap = new HashMap<String,Map<String,String>>();
	
	private static Map<String,Map<String,Object>> trackingMap = new HashMap<String,Map<String,Object>>();
	
	private static Map<String,Set<String>> userProdIdDBMap = new HashMap<String,Set<String>>();
	
	private static String uuid = null;
	
	private static String paclType = null;
	
	private static String jobDate = null;
	
	private Context context = null;
	
	private HBaseAdmin admin = null;
	
	private Configuration conf = null;
	
	private static StringBuffer findAdactionReportUserSql = new StringBuffer();
	
	private static StringBuffer findEcProdrSql = new StringBuffer();
	
	private static boolean prodAdFlag = false;
	
	private static boolean notProdAdFlag = false;
	
	private static boolean prodFlag = false;
	
	
	private static String pfpCustomerInfoId	 = "";
	
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			jobDate = context.getConfiguration().get("job.date");
			//MySql
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(context.getConfiguration().get("spring.profiles.active"));
			
			//HBASE
			conf = HBaseConfiguration.create();
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.2.150,192.168.2.151,192.168.2.152");
			conf.set("hbase.zookeeper.property.clientPort", "3333");
			conf.set("hbase.master", "192.168.2.149:16010");   
			conf = HBaseConfiguration.create(conf);
			Connection connection = ConnectionFactory.createConnection(conf);
			this.admin = (HBaseAdmin) connection.getAdmin();
			
			findAdactionReportUserSql.append(" SELECT  ");
			findAdactionReportUserSql.append(" tracking_seq, ");
			findAdactionReportUserSql.append(" pfp_customer_info_id, ");
			findAdactionReportUserSql.append(" r.ad_operating_rule ");
			findAdactionReportUserSql.append(" FROM   pfp_code_tracking t  ");
			findAdactionReportUserSql.append(" LEFT JOIN pfp_ad_action_report r ");
			findAdactionReportUserSql.append(" ON t.pfp_customer_info_id = r.customer_info_id  ");
			findAdactionReportUserSql.append(" AND r.ad_pvclk_date = '").append(jobDate).append("'");
			findAdactionReportUserSql.append(" WHERE  1 = 1  ");
			findAdactionReportUserSql.append(" AND t.tracking_seq = 'REPLACE_TRACKING_ID' GROUP BY ad_operating_rule  ");
			
			findEcProdrSql.append(" SELECT ec.catalog_prod_seq, ");
			findEcProdrSql.append(" ca.pfp_customer_info_id ");
			findEcProdrSql.append(" FROM   pfp_catalog ca ");
			findEcProdrSql.append(" RIGHT JOIN pfp_catalog_prod_ec ec ");
			findEcProdrSql.append(" ON ca.catalog_seq = ec.catalog_seq ");
			findEcProdrSql.append(" AND ec.ec_status = '1' ");
			findEcProdrSql.append("  AND ec.ec_check_status = '1' ");
			findEcProdrSql.append(" WHERE  1 = 1 ");
			findEcProdrSql.append(" AND ca.pfp_customer_info_id = 'REPLACE_CUSTOMER_ID'  ");
			findEcProdrSql.append(" AND ca.catalog_delete_status = '0'  ");
			findEcProdrSql.append(" AND ca.upload_status = '2'  ");
			
			
			
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}

	@SuppressWarnings("static-access")
	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			this.context = context;
			uuid = null;
			
			String key = mapperKey.toString();
			uuid = key.split("<PCHOME>",-1)[1];
			paclType = key.split("<PCHOME>",-1)[2];
			//處理轉換資料
			if(paclType.equals("convert")){
				convertSeq = key.split("<PCHOME>",-1)[0];
				userDefineConvertPrice = null;
				convertConditionArray = null;
				for (Text text : mapperValue) {
					String value = text.toString();
					JSONObject logJson = (JSONObject) jsonParser.parse(value);
					String userDefineConvertPrice = logJson.getAsString("userDefineConvertPrice");
					if(StringUtils.isNotBlank(userDefineConvertPrice) && Pattern.compile("^[0-9]+$").matcher(userDefineConvertPrice).find()){
						this.userDefineConvertPrice = Integer.parseInt(userDefineConvertPrice);
					}
					paclLogList.add(logJson);
				}
				processConvertLog();
			}
			//處理追蹤資料
			if(paclType.equals("tracking")){
				this.prodFlag = false;
				this.prodAdFlag = false;
				this.notProdAdFlag = false;
				this.pfpCustomerInfoId = "";
				this.trackingSeq = key.split("<PCHOME>",-1)[0];
				Map<String,Object> trackingDeatilMap = null;
				if(trackingMap.containsKey(trackingSeq)){
					trackingDeatilMap = trackingMap.get(trackingSeq);
					this.pfpCustomerInfoId = (String) trackingDeatilMap.get("pfpCustomerInfoId");
				}else{
					ResultSet resultSet = mysqlUtil.query(findAdactionReportUserSql.toString().replace("REPLACE_TRACKING_ID", trackingSeq));
					while(resultSet.next()){
						String adOperatingRule = resultSet.getString("ad_operating_rule");
						this.pfpCustomerInfoId = resultSet.getString("pfp_customer_info_id");
						if(StringUtils.isNotBlank(adOperatingRule)){
							if(StringUtils.isNotBlank(adOperatingRule) && adOperatingRule.equals("PROD")){
								prodAdFlag = true;
							}else if(StringUtils.isNotBlank(adOperatingRule) && !adOperatingRule.equals("PROD")){
								notProdAdFlag = true;
							}
						}
					}
					trackingDeatilMap = new HashMap<String,Object>();
					trackingDeatilMap.put("adProdOperatingFlag", prodAdFlag);
					trackingDeatilMap.put("adNotProdOperatingFlag", notProdAdFlag);
					trackingDeatilMap.put("pfpCustomerInfoId", pfpCustomerInfoId);
					trackingMap.put(trackingSeq, trackingDeatilMap);
				}
				
				//	saveHbaseTrackingMap
				for (Text text : mapperValue) {
					String prodId = text.toString().split("<PCHOME>",-1)[0];
					String trackingDate = text.toString().split("<PCHOME>",-1)[1];
					this.prodFlag = false;
					this.prodAdFlag = (boolean) trackingDeatilMap.get("adProdOperatingFlag");
					this.notProdAdFlag = (boolean) trackingDeatilMap.get("adNotProdOperatingFlag");
					//1.非商品廣告資料 2.pfp_adaction_report有資料
					if(StringUtils.isBlank(prodId) && this.notProdAdFlag){
						if(saveHbaseTrackingMap.containsKey(uuid)){
							Map<String,String> saveHbaseTrackingDetail = saveHbaseTrackingMap.get(uuid);
							saveHbaseTrackingDetail.put(this.trackingSeq+"_ALL", trackingDate);
						}else{
							Map<String,String> saveHbaseTrackingDetail = new HashMap<String,String>();
							saveHbaseTrackingDetail.put(this.trackingSeq+"_ALL", trackingDate);
							saveHbaseTrackingMap.put(uuid, saveHbaseTrackingDetail);
						}
					}else if(StringUtils.isNotBlank(prodId) && this.prodAdFlag){ 	//1.商品廣告資料 2.判斷商品存在
						if(!userProdIdDBMap.containsKey(this.pfpCustomerInfoId)){
							ResultSet resultSet = mysqlUtil.query(findEcProdrSql.toString().replace("REPLACE_CUSTOMER_ID", this.pfpCustomerInfoId));
							Set<String> prodSet = new HashSet<String>();
							while(resultSet.next()){
								String dbProdSeq = resultSet.getString("catalog_prod_seq");
								prodSet.add(dbProdSeq);
							}
							userProdIdDBMap.put(this.pfpCustomerInfoId, prodSet);
						}
						/*判斷商品是否存在開始*/
						Set<String> prodSet = userProdIdDBMap.get(this.pfpCustomerInfoId);
						for (String dbProdId : prodSet) {
							if(prodId.equals(dbProdId)){
								prodFlag = true;
								break;
							}
						}
						
						if(prodFlag){
							if(saveHbaseTrackingMap.containsKey(uuid)){
								Map<String,String> saveHbaseTrackingDetail = saveHbaseTrackingMap.get(uuid);
								saveHbaseTrackingDetail.put(this.trackingSeq+"_"+prodId, trackingDate);
							}else{
								Map<String,String> saveHbaseTrackingDetail = new HashMap<String,String>();
								saveHbaseTrackingDetail.put(this.trackingSeq+"_"+prodId, trackingDate);
								saveHbaseTrackingMap.put(uuid, saveHbaseTrackingDetail);
							}
						}
						/*判斷商品是否存在結束*/
					}
				}
			}
			
			paclLogList.clear();
			convertConditionSet.clear();
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			convertResultMap.clear();
			
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	
	
	/**
	 * 1.取得轉換條件
	 * 2.根據轉換條件各自需踩過一次log
	 * 3.
	 * */
	@SuppressWarnings("static-access")
	public void processConvertLog() throws Exception{
		if(convertConditionMap.get(convertSeq) == null){
			sql.append(" SELECT   ");
			sql.append(" 	c.convert_type,  ");
			sql.append(" 	c.convert_seq,  ");
			sql.append(" 	c.click_range_date,  ");
			sql.append(" 	c.imp_range_date,  ");
			sql.append(" 	c.convert_price,  ");
			sql.append(" 	c.convert_status,  ");
			sql.append(" 	c.convert_belong,  ");
			sql.append(" 	c.convert_num_type,  ");
			sql.append(" 	Group_concat(convert_rule_id SEPARATOR ':')convert_rule_id,  ");
			sql.append(" 	c.pfp_customer_info_id  ");
			sql.append(" FROM   pfp_code_convert c  ");
			sql.append(" LEFT JOIN pfp_code_convert_rule r  ");
			sql.append(" ON( c.convert_seq = r.convert_seq )  ");
			sql.append(" WHERE  1 = 1  ");
			sql.append(" AND c.convert_seq = '").append(convertSeq).append("'");
			sql.append(" GROUP  BY r.convert_seq  ");
			
			ResultSet resultSet = mysqlUtil.query(sql.toString());
			while(resultSet.next()){
				int clickRangeDate = resultSet.getInt("click_range_date");
				int impRangeDate = resultSet.getInt("imp_range_date");
				String convertPrice = resultSet.getString("convert_price");
				String convertStatus = resultSet.getString("convert_status");
				String convertBelong = resultSet.getString("convert_belong");
				int convertNumType = resultSet.getInt("convert_num_type");
				String convertRule = resultSet.getString("convert_rule_id");
				String convertType = resultSet.getString("convert_type");
				
				pcalConditionBean = new PcalConditionBean();
				pcalConditionBean.setClickRangeDate(clickRangeDate);
				pcalConditionBean.setImpRangeDate(impRangeDate);
				pcalConditionBean.setConvertPrice(convertPrice);
				pcalConditionBean.setConvertStatus(convertStatus);
				pcalConditionBean.setConvertNumType(convertNumType);
				pcalConditionBean.setConvertBelong(convertBelong);
				pcalConditionBean.setConvertRule(convertRule);
				pcalConditionBean.setConvertType(convertType);
				convertConditionMap.put(convertSeq, pcalConditionBean);
			}
		}else{
			pcalConditionBean = convertConditionMap.get(convertSeq);
		}
		
//		整理條件內容與總計(_0)
//		ConvertType 1:標準轉換 2:自訂轉換
		if(pcalConditionBean.getConvertType().equals("1")){
			convertConditionSet.add("ALL_0");
		}else if(pcalConditionBean.getConvertType().equals("2")){
			convertConditionArray = pcalConditionBean.getConvertRule().split(":");
			for (String rouleId : convertConditionArray) {
				convertConditionSet.add(rouleId+"_0");
			}
		}
		//開始計算條件出現次數
		for (JSONObject paclLogJson : paclLogList) {
			for (String convertConditionStr : convertConditionSet) {
				String converArray[] = convertConditionStr.split("_");
				String rouleId = converArray[0];
				int count = Integer.parseInt(converArray[1]);
				if(pcalConditionBean.getConvertType().equals("1")){
					convertConditionSet.remove(rouleId+"_"+count);
					count ++;
					String data = rouleId+"_"+String.valueOf(count);
					convertConditionSet.add(data);
					break;
				}else if(pcalConditionBean.getConvertType().equals("2")){
					if(rouleId.equals(paclLogJson.getAsString("rouleId"))){
						convertConditionSet.remove(rouleId+"_"+count);
						count ++;
						String data = rouleId+"_"+String.valueOf(count);
						convertConditionSet.add(data);
						break;
					}
				}
			}
		}
		//統計轉換次數
		int convertCount = 0;
		for (String rouleIdCountStr : convertConditionSet) {
			String converArray[] = rouleIdCountStr.split("_");
			int count = Integer.parseInt(converArray[1]);
			if(count == 0){
				convertCount = 0;
				break;
			}
			if(pcalConditionBean.getConvertType().equals("1")){
				convertCount = count;
			}else if(pcalConditionBean.getConvertType().equals("2")){
				if(convertCount == 0){
					convertCount = count;
				}else if(count < convertCount){
					convertCount = count;
				}
			}
		}
		
		//轉換數須大於0才輸出lzo
		if(convertCount > 0){
			int convertPriceCount = 0;
			//1:每次 2:一次
			if(pcalConditionBean.getConvertNumType() == 1){
				pcalConditionBean.setConvertCount(convertCount);
				if(this.userDefineConvertPrice != null){
					convertPriceCount = this.userDefineConvertPrice * convertCount;
				}else{
					convertPriceCount = (int)Double.parseDouble(pcalConditionBean.getConvertPrice()) * convertCount;
				}
			}else if(pcalConditionBean.getConvertNumType() == 2){
				pcalConditionBean.setConvertCount(1);
				if(this.userDefineConvertPrice != null){
					convertPriceCount = this.userDefineConvertPrice;
				}else{
					convertPriceCount = (int)Double.parseDouble(pcalConditionBean.getConvertPrice());
				}
			}
			keyOut.set(uuid);
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getClickRangeDate());
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getImpRangeDate());
			convertWriteInfo.append(paclSymbol).append(convertPriceCount);
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getConvertPrice());
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getConvertBelong());
			convertWriteInfo.append(paclSymbol).append(convertSeq);
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getConvertNumType());
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getConvertCount());
			convertWriteInfo.append(paclSymbol).append(jobDate);
			context.write(keyOut, new Text(convertWriteInfo.toString()));
		}
	}
	
	
	@SuppressWarnings({ "deprecation", "resource" })
	 public void putData(String tableName,String rowKey,String family,String qualifier,JSONObject logJson) throws Exception{
		 log.info("save hbase>>>>>>>>>tableName:"+tableName+">>>>>rowKey:"+rowKey+">>>>>>family:"+family+">>>>>>>>qualifier:"+qualifier+">>>>>>>>logJson:"+logJson);
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 int region = Math.abs(rowKey.hashCode()) % 10;
		 rowKey = "0"+region+"|"+rowKey;
		 Get get = new Get(Bytes.toBytes(rowKey));
		 get.addColumn(Bytes.toBytes("type"), Bytes.toBytes("retargeting"));
		 Result result = table.get(get);
		 String row = Bytes.toString(result.getRow());
		 
		 if(row != null){
			 JSONObject hbaseValueJson = (JSONObject) jsonParser.parse(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
			 //刪除超過28天資料
			 for (Entry<String, Object> entry : hbaseValueJson.entrySet()) {
				 System.out.println(entry.getKey());
				 System.out.println(entry.getValue());
				 double days = (double) ((now.getTime() - sdfFormat.parse(entry.getValue().toString()).getTime()) / (1000*3600*24));
				 if(days > 28){
					 hbaseValueJson.remove(entry.getKey());
				 }
			 }
			 //資料天數小於28天才會寫入更新
			 for (Entry<String, Object> entry : logJson.entrySet()) {
				 double days = (double) ((now.getTime() - sdfFormat.parse(entry.getValue().toString()).getTime()) / (1000*3600*24));
				 
				 log.info("save hbase >>>>>days:"+days);
				 log.info("save hbase >>>>>entry.getKey():"+entry.getKey()+">>>>>>>>>>entry.getValue().toString():"+entry.getValue().toString());
				 if(days <= 28){
					 hbaseValueJson.put(entry.getKey(), entry.getValue().toString());
				 }
			 }
			 Put put = new Put(Bytes.toBytes(rowKey));
			 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(logJson.toString()));
			 table.put(put);
		 }else{
			 Put put = new Put(Bytes.toBytes(rowKey));
			 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(logJson.toString()));
			 table.put(put);
		 }
	 }
	 
	 @SuppressWarnings({ "deprecation", "resource" })
	 public JSONObject getData(String tableName,String rowKey,String family,String qualifier) throws Exception{
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 int region = Math.abs(rowKey.hashCode()) % 10;
		 rowKey = "0"+region+"|"+rowKey;
		 Get get = new Get(Bytes.toBytes(rowKey));
		 get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		 Result result = table.get(get);
		 return (JSONObject) jsonParser.parse(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
	 }
	
	 public void cleanup(Context context) {
		try {
			log.info("saveHbaseTrackingMap:"+saveHbaseTrackingMap);
			JSONObject data = new JSONObject();
			for (Entry<String, Map<String, String>> entry : saveHbaseTrackingMap.entrySet()) {
				String rowKey = entry.getKey();
				Map<String,String> saveHbaseTrackingDetail = entry.getValue();
				for (Entry<String, String> saveHbaseTrackingData : saveHbaseTrackingDetail.entrySet()) {
					data.clear();
					data.put(saveHbaseTrackingData.getKey(), saveHbaseTrackingData.getValue());
					putData("pacl_retargeting",rowKey,"type","retargeting",data);
				}
			}
			PreparedStatement preparedStmt = mysqlUtil.getConnect().prepareStatement( "DELETE FROM `pfp_code_convert_trans` where  1=1 and convert_date = '"+jobDate+"'");
			preparedStmt.execute();
			mysqlUtil.getConnect().commit();
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			convertConditionSet.clear();
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
}