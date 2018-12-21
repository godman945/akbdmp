package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import com.pchome.soft.util.HBaseUtil;
import com.pchome.soft.util.MysqlUtil;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class PaclLogConverCountReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("PaclLogConverCountReducer");

	private Text keyOut = new Text();

	private Text valueOut = new Text();

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
	
	private HBaseUtil hbaseUtil = null;
	
	
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
	
	private static String uuid = null;
	
	private static String paclType = null;
	
	private static String jobDate = null;
	
	private Context context = null;
	
	private HBaseAdmin admin = null;
	
	private Configuration conf = null;
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			jobDate = context.getConfiguration().get("job.date");
			log.info(">>>>>>>>>>>jobDate:"+jobDate);
			//MySql
			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "keyword";
			String password =  "K1y0nLine";
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(url, user, password);
			//HBASE
			Configuration conf = HBaseConfiguration.create();
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.2.150,192.168.2.151,192.168.2.152");
			conf.set("hbase.zookeeper.property.clientPort", "3333");
			conf.set("hbase.master", "192.168.2.149:16010");   
			conf = HBaseConfiguration.create(conf);
			Connection connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			this.context = context;
			uuid = null;
			convertSeq = null;
			userDefineConvertPrice = null;
			convertConditionArray = null;
			
			String key = mapperKey.toString();
			log.info(">>>>>>init mapperKey:"+key);
			
			convertSeq = key.split("<PCHOME>")[0];
			uuid = key.split("<PCHOME>")[1];
			paclType = key.split("<PCHOME>")[2];
			
			//處理轉換資料
			if(paclType.equals("convert")){
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
			
			if(paclType.equals("tracking")){
				
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
	
	
	
	public void processConvertLog() throws Exception{
		if(convertConditionMap.get(convertSeq) == null){
			log.info(">>>>>>convertConditionMap data not exist!!");
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
//				log.info(">>>>>>convertConditionMap:"+pfpCustomerInfoId);
			}
		}else{
			log.info(">>>>>>convertConditionMap data exist!!");
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
		
		log.info("convertSeq:"+convertSeq+">>>>>>>convertConditionSet:"+convertConditionSet.toString());
		log.info("paclLogList:"+paclLogList.toString());
		
		
//		開始計算條件出現次數
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
		log.info("convertSeq:"+convertSeq+">>>>>>>convertConditionSet:"+convertConditionSet.toString());
		
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
//			log.info("============="+convertConditionSet+" convert count:"+convertCount);
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
			log.info(">>>>>>write:"+convertWriteInfo.toString());
			context.write(keyOut, new Text(convertWriteInfo.toString()));
		}
	}
	
	public void processTrackingLog() throws Exception{
		
	}
	
	
	
	 public void putData(String tableName,String rowKey,String family,String qualifier,JSONObject logJson) throws Exception{
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 int region = Math.abs(rowKey.hashCode()) % 10;
		 rowKey = "0"+region+"|"+rowKey;
		 Get get = new Get(Bytes.toBytes(rowKey));
		 get.addColumn(Bytes.toBytes("type"), Bytes.toBytes("retargeting"));
		 Result result = table.get(get);
		 String row = Bytes.toString(result.getRow());
		 
		 //row存在更新value否則建立一筆新row
		 if(row != null){
			 JSONObject hbaseValueJson = (JSONObject) jsonParser.parse(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
			 for (Entry<String, Object> entry : logJson.entrySet()) {
				 String logkey = entry.getKey();
				 String logValue = (String) entry.getValue();
				 hbaseValueJson.put(logkey, logValue);
			 }
			 Put put = new Put(Bytes.toBytes(rowKey));
			 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(hbaseValueJson.toString()));
			 table.put(put);
		 }else{
			 Put put = new Put(Bytes.toBytes(rowKey));
			 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(logJson.toString()));
			 table.put(put);
		 }
	 }
	 
	 public JSONObject getData(String tableName,String rowKey,String family,String qualifier) throws Exception{
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 int region = Math.abs(rowKey.hashCode()) % 10;
		 rowKey = "0"+region+"|"+rowKey;
		 Get get = new Get(Bytes.toBytes(rowKey));
		 get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		 Result result = table.get(get);
		 String row = Bytes.toString(result.getRow());
		 if(row == null){
			 return null;
		 }else{
			 return (JSONObject) jsonParser.parse(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
		 }
	 }
	
	
	
	
	public void cleanup(Context context) {
		try {
			
			
//			
//			 HTable table = new HTable(conf, Bytes.toBytes(tableName));
//			 int region = Math.abs(rowKey.hashCode()) % 10;
//			 rowKey = "0"+region+"|"+rowKey;
//			 Get get = new Get(Bytes.toBytes(rowKey));
//			 get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
//			 Result result = table.get(get);
//			 String row = Bytes.toString(result.getRow());
//			
//			 log.info("************************************************"+Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
//			 log.info("-----------------------");
//			 org.json.JSONObject ja = new org.json.JSONObject(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
//			 log.info("-----------------------"+ja);
			
			hbaseUtil = HBaseUtil.getInstance();
//			String a = "192.168.2.150,192.168.2.151,192.168.2.152";
//			String b = "3333";
//			String c = "192.168.2.149:16010";
//			hbaseUtil.initHbaseConfig(a,b,c);
//   		 	log.info(">>>>>>>>>>>>>>>>hbaseValue:"+hbaseUtil.getData("pacl_retargeting", "alex", "type", "retargeting"));
			
			
			
//			PreparedStatement preparedStmt = mysqlUtil.getConnect().prepareStatement( "DELETE FROM `pfp_code_convert_trans` where  1=1 and convert_date = '"+jobDate+"'");
//			preparedStmt.execute();
//			mysqlUtil.getConnect().commit();
//			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			convertConditionSet.clear();
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	
	
}
