package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.springframework.stereotype.Component;

import net.minidev.json.JSONObject;

@Component
public class PaclLogConverCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("PaclLogConverCountMapper");
	
	
	private Text keyOut = new Text();

	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	
	private static SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
	
	private static JSONObject kdclInfo = new JSONObject();
	
	private static JSONObject paclInfo = new JSONObject();
	
	private static JSONObject paclLogInfo = new JSONObject();
	
	public static StringBuffer effectPaclPfpUser = new StringBuffer();
	
	public static String paclUuid = null;
	
	
	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			effectPaclPfpUser.append(context.getConfiguration().get("effectPaclPfpUser"));
//			log.info(">>>>>>>>>effectPaclPfpUser:"+effectPaclPfpUser.toString());
			paclUuid = context.getConfiguration().get("paclUuid");
//			log.info(">>>>>>>>>paclUuid:"+paclUuid);
			
		} catch (Exception e) {
			log.error("Mapper setup error>>>>>> " +e);
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		try {
			InputSplit inputSplit=(InputSplit)context.getInputSplit(); 
			String fileName = ((FileSplit)inputSplit).getPath().getName();
			String valueStr = value.toString();
			String arrayData[] = valueStr.split(paclSymbol,-1);
//			log.info(">>>>>filename:"+fileName);
			//載入的檔案為pacl(排程1)
			if(fileName.contains("pacl")){
				String paclType = arrayData[11];
//				log.info("raw_data : " + valueStr);
//				log.info("arrayData size : " + arrayData.length);
//				log.info("paclType:"+paclType);
				if(paclType.equals("tracking")){
					String paclUuid = arrayData[2];
					if(StringUtils.isBlank(paclUuid)) {
						return;
					}
					String trackingId = arrayData[12];
					String prodId = arrayData[13];
					String date = arrayData[0];
					keyOut.set(trackingId+"<PCHOME>"+paclUuid+"<PCHOME>"+paclType);
					context.write(keyOut, new Text(prodId+"<PCHOME>"+date));
				}else if(paclType.equals("page_view")){
					
				}else if(paclType.equals("convert")){
					String paclUuid = arrayData[2];
					String convId = arrayData[12];
					String rouleId = arrayData[13];
					String userDefineConvertPrice = arrayData[14];
					if(StringUtils.isBlank(paclUuid)) {
						return;
					}
					paclLogInfo.put("paclUuid", paclUuid);
					paclLogInfo.put("paclType", paclType);
					paclLogInfo.put("convId", convId);
					paclLogInfo.put("rouleId", rouleId.replace(";", ""));
					paclLogInfo.put("userDefineConvertPrice", userDefineConvertPrice);
					keyOut.set(convId+"<PCHOME>"+paclUuid+"<PCHOME>"+paclType);
					context.write(keyOut, new Text(paclLogInfo.toString()));
				}
			}else if(fileName.contains("kdcl") || fileName.contains("kwstg")){
					String pfpCustomerInfoId = arrayData[6];
					String kdclUUid = arrayData[2];
					if(StringUtils.isBlank(kdclUUid)) {
						return;
					}
					if(effectPaclPfpUser.toString().contains(pfpCustomerInfoId) && paclUuid.contains(kdclUUid)){
//						log.info(">>>>>>kdcl log");
//						log.info("raw_data : " + value);
						String date = arrayData[0];
						String times = String.valueOf(sdf.parse(date).getHours());
						if(times.length() == 1){
							times = 0+times;
						}
						String formatDate = sdf2.format(sdf.parse(date));
						String referer = arrayData[4];
						String userAgent = arrayData[5];
						String pfbxCustomerInfoId = arrayData[25];
						String pfbxPositionId = arrayData[26];
						String pfdCustomerInfoId = arrayData[23];
						String payType = arrayData[29];
						String sex = arrayData[31];
						String age = arrayData[32];
//						String timeCode = arrayData[33];
						String categoryCode = arrayData[35];
						String priceType = arrayData[36];
						String styleId = arrayData[7];
						String tproId = arrayData[8];
						String uuid = arrayData[2];
						String adSeq = arrayData[11];
						String type = arrayData[13];
						String adType = arrayData[14];
						String actionSeq = arrayData[21];
						String groupSeq = arrayData[22];
						
						kdclInfo.put("kdclSourceDate", date);
						kdclInfo.put("kdclDate", formatDate);
						kdclInfo.put("keclTime", times);
						kdclInfo.put("pfpCustomerInfoId", pfpCustomerInfoId);
						kdclInfo.put("pfbxCustomerInfoId",pfbxCustomerInfoId );
						kdclInfo.put("pfbxPositionId", pfbxPositionId);
						kdclInfo.put("pfdCustomerInfoId", pfdCustomerInfoId);
						kdclInfo.put("payType", payType);
						kdclInfo.put("sex",sex);
						kdclInfo.put("age", age);
//						kdclInfo.put("timeCode",timeCode);
						kdclInfo.put("styleId",styleId);
						kdclInfo.put("uuid",uuid);
						kdclInfo.put("adSeq", adSeq);
						kdclInfo.put("kdclType", type);
						kdclInfo.put("adType", adType);
						kdclInfo.put("actionSeq", actionSeq);
						kdclInfo.put("groupSeq", groupSeq);
						kdclInfo.put("referer", referer);
						kdclInfo.put("userAgent", userAgent);
						kdclInfo.put("fileName", fileName);
						kdclInfo.put("tproId", tproId);
						kdclInfo.put("categoryCode", categoryCode);
						kdclInfo.put("priceType", priceType);
						keyOut.set(uuid);
						context.write(keyOut, new Text(kdclInfo.toString()));
					}
				}else if(fileName.contains("part")){
//					log.info(">>>>>>conv log");
//					log.info("raw_data : " + value);
					String uuid = arrayData[0].trim();
					String clickRangeDate = arrayData[1];
					String impRangeDate = arrayData[2];
					String convertPriceCount = arrayData[3];
					String convertPrice = arrayData[4];
					String convertBelong = arrayData[5];
					String convertSeq = arrayData[6];
					String convertNumType = arrayData[7];
					String convertCount = arrayData[8];
					
					paclInfo.put("clickRangeDate",clickRangeDate);
					paclInfo.put("impRangeDate",impRangeDate);
					paclInfo.put("convertPriceCount",convertPriceCount);
					paclInfo.put("convertPrice",convertPrice);
					paclInfo.put("convertBelong",convertBelong);
					paclInfo.put("convertSeq",convertSeq);
					paclInfo.put("fileName",fileName);
					paclInfo.put("convertNumType",convertNumType);
					paclInfo.put("convertCount",convertCount);
					keyOut.set(uuid);
					context.write(keyOut, new Text(paclInfo.toString()));
			}
			paclLogInfo.clear();
			paclInfo.clear();
			kdclInfo.clear();
		} catch (Exception e) {
			kdclInfo.clear();
			paclInfo.clear();
			log.error("Mapper error>>>>>> " +e); 
		}
	}
	
	
}
