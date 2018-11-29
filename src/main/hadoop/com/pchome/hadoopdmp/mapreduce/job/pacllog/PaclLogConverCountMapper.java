package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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
	Log log = LogFactory.getLog("DmpLogMapper");
	
	private Text keyOut = new Text();

	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	
	private static SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
	
	private static JSONObject kdclInfo = new JSONObject();
	
	private static JSONObject paclInfo = new JSONObject();
	
	private static Calendar calendar = null;
	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			calendar = Calendar.getInstance();
			calendar.setTime(new Date());
			calendar.add(Calendar.DAY_OF_MONTH, -1); 
		} catch (Exception e) {
			log.error("Mapper setup error>>>>>> " +e);
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		try {
			InputSplit inputSplit=(InputSplit)context.getInputSplit(); 
			String filename = ((FileSplit)inputSplit).getPath().getName();
			String valueStr = value.toString();
			String arrayData[] = valueStr.split(paclSymbol);
//			log.info("Path:"+((FileSplit)inputSplit).getPath());
//			log.info("filename:"+filename);
			if(filename.equals("pacl_2018_11_26.txt.lzo")){
//				log.info("raw_data : " + valueStr);
//				log.info("arrayData size : " + arrayData.length);
				String uuid = arrayData[2];
				String type = arrayData[11];
				String convId = arrayData[12];
				String rouleId = arrayData[13];
				if(type.equals("convert")){
					keyOut.set(convId+"_"+uuid);
					context.write(keyOut, new Text(rouleId.replace(";", "")));
				}
			}else{
				if(filename.contains("kdcl")){
					log.info(">>>>>>kdcl log");
					log.info("raw_data : " + value);
//					log.info("arrayData size : " + arrayData.length);
					String date = arrayData[0];
					String times = String.valueOf(sdf.parse(date).getHours());
					if(times.length() == 1){
						times = 0+times;
					}
					date = sdf2.format(sdf.parse(date));
					String pfpCustomerInfoId = arrayData[6];
					String pfbxCustomerInfoId = arrayData[25];
					String pfbxPositionId = arrayData[26];
					String pfdCustomerInfoId = arrayData[23];
					String payType = arrayData[29];
					String sex = arrayData[31];
					String ageCode = arrayData[32];
					String timeCode = arrayData[33];
					String styleId = arrayData[7];
					String uuid = arrayData[2];
					String adSeq = arrayData[11];
					String type = arrayData[13];
					String adType = arrayData[14];
					String actionSeq = arrayData[21];
					String groupSeq = arrayData[22];
					
					kdclInfo.put("kdclDate", date);
					kdclInfo.put("keclTime", times);
					kdclInfo.put("pfpCustomerInfoId", pfpCustomerInfoId);
					kdclInfo.put("pfbxCustomerInfoId",pfbxCustomerInfoId );
					kdclInfo.put("pfbxPositionId", pfbxPositionId);
					kdclInfo.put("pfdCustomerInfoId", pfdCustomerInfoId);
					kdclInfo.put("payType", payType);
					kdclInfo.put("sex",sex);
					kdclInfo.put("ageCode", ageCode);
					kdclInfo.put("timeCode",timeCode);
					kdclInfo.put("styleId",styleId);
					kdclInfo.put("uuid",uuid);
					kdclInfo.put("adSeq", adSeq);
					kdclInfo.put("type", type);
					kdclInfo.put("adType", adType);
					kdclInfo.put("actionSeq", actionSeq);
					kdclInfo.put("groupSeq", groupSeq);
					kdclInfo.put("filename", filename);
					keyOut.set(uuid);
					context.write(keyOut, new Text(kdclInfo.toString()));
				}else{
					log.info(">>>>>>conv log");
					log.info("raw_data : " + value);
//					log.info("arrayData size : " + arrayData.length);
					String uuid = arrayData[0].trim();
					String clickRangeDate = arrayData[1];
					String impRangeDate = arrayData[2];
					String convertPriceCount = arrayData[3];
					String convertPrice = arrayData[4];
					String convertBelong = arrayData[5];
					String convertSeq = arrayData[6];
					paclInfo.put("clickRangeDate",clickRangeDate);
					paclInfo.put("impRangeDate",impRangeDate);
					paclInfo.put("convertPriceCount",convertPriceCount);
					paclInfo.put("convertPrice",convertPrice);
					paclInfo.put("convertBelong",convertBelong);
					paclInfo.put("convertSeq",convertSeq);
					paclInfo.put("filename",filename);
					keyOut.set(uuid);
					context.write(keyOut, new Text(paclInfo.toString()));
				}
			}
			
			paclInfo.clear();
			kdclInfo.clear();
		} catch (Exception e) {
			kdclInfo.clear();
			paclInfo.clear();
			log.error("Mapper error>>>>>> " +e); 
		}
	}
}
