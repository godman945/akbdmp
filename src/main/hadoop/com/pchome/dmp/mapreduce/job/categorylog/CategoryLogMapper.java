package com.pchome.dmp.mapreduce.job.categorylog;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import com.pchome.dmp.enumerate.CategoryLogEnum;
import com.pchome.dmp.mapreduce.job.factory.ACategoryLogData;
import com.pchome.dmp.mapreduce.job.factory.CategoryLogBean;
import com.pchome.dmp.mapreduce.job.factory.CategoryLogFactory;

@Component
public class CategoryLogMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static int LOG_LENGTH = 30;
	private static String SYMBOL = String.valueOf(new char[] { 9, 31 });
	// private Log log = LogFactory.getLog(this.getClass());
	Log log = LogFactory.getLog("CategoryLogMapper");

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;
	public static CategoryLogBean categoryLogBean;
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();
	public static ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
	

	@Override
	public void setup(Context context) {
		record_date = context.getConfiguration().get("job.date");
		this.categoryLogBean = new CategoryLogBean();
		record_date = context.getConfiguration().get("job.date");

		Configuration conf = context.getConfiguration();
		try {
			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
			Path clsfyTable = Paths.get(path[1].toString());
			Charset charset = Charset.forName("UTF-8");
			List<String> lines = Files.readAllLines(clsfyTable, charset);
			for (String line : lines) {
				String[] tmpStrAry = line.split(";"); // 0001000000000000;M,35
				String[] tmpStrAry2 = tmpStrAry[1].split(",");
				clsfyCraspMap.put(tmpStrAry[0],	new combinedValue(tmpStrAry[1].split(",")[0], tmpStrAry2.length > 1 ? tmpStrAry2[1] : ""));
			}
			
			// get csv file
			Path cate_path = Paths.get(path[0].toString());
			charset = Charset.forName("UTF-8");

			int maxCateLvl = 4;
			list = new ArrayList<Map<String, String>>();

			for (int i = 0; i < maxCateLvl; i++) {
				list.add(new HashMap<String, String>());
			}
			
			lines.clear();
			lines = Files.readAllLines(cate_path, charset);

			// 將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
			for (String line : lines) {
				String[] tmpStr = line.split(";");
				int lvl = Integer.parseInt(tmpStr[5].replaceAll("\"", "").trim());
				if (lvl <= maxCateLvl) {
					list.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
				}

			}
		} catch (Exception e) {
			// log.error("ClsfyGndAgeCrspTable error:\n" + e.getMessage());
			// e.printStackTrace();
			// log.error("ClsfyGndAgeCrspTable error:\n" + e.pr);
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		Path cate_path = Paths.get("D:/home/webuser/pfp_ad_category_new.csv");
		Path clsfyTable = Paths.get("D:/home/webuser/ClsfyGndAgeCrspTable.txt");
		Charset charset = Charset.forName("UTF-8");
		
		try {
			
			List<String> lines = Files.readAllLines(clsfyTable, charset);
			for (String line : lines) {
				String[] tmpStrAry = line.split(";"); // 0001000000000000;M,35
				String[] tmpStrAry2 = tmpStrAry[1].split(",");
				clsfyCraspMap.put(tmpStrAry[0],	new combinedValue(tmpStrAry[1].split(",")[0], tmpStrAry2.length > 1 ? tmpStrAry2[1] : ""));
			}
			
			// get csv file

			int maxCateLvl = 4;
			list = new ArrayList<Map<String, String>>();

			for (int i = 0; i < maxCateLvl; i++) {
				list.add(new HashMap<String, String>());
			}
			
			lines.clear();
			lines = Files.readAllLines(cate_path, charset);

			// 將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
			for (String line : lines) {
				String[] tmpStr = line.split(";");
				int lvl = Integer.parseInt(tmpStr[5].replaceAll("\"", "").trim());
				if (lvl <= maxCateLvl) {
					list.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
				}

			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		
		
		
		
		// log.info("value=" + value);

//		if (StringUtils.isBlank(value.toString())) {
//			// log.info("value is blank");
//			return;
//		}
//
//		String[] values = value.toString().split(SYMBOL);
//		if (values.length < LOG_LENGTH) {
//			log.info("values.length < " + LOG_LENGTH);
//			return;
//		}
//
//		AncestorJob job = null;
		String key = null;
		String val = null;

		// 1.
		// values[1] //mid
		// values[2] //uuid
		// values[13] //ck,pv
		// values[4] //url
		// values[15] //ad_class
		// values[3] //behavior
		
		String [] values =  new String[100];
		values[1] = "sts0330";
		values[2] = "alex2";
		values[13] = "pv";
		values[4] = "http://24h.pchome.com.tw/region/DICM";
		values[15] = "alex5";
		values[3] = "alex6";
		
 		log.info(">>>>>> "+values[1]);
		log.info(">>>>>> "+values[2]);
		log.info(">>>>>> "+values[13]);
		log.info(">>>>>> "+values[4]);
		log.info(">>>>>> "+values[15]);
		log.info(">>>>>> "+values[3]);
		log.info("----------------------------------- data");
		try {

//			String domain = "";
//			Pattern p = Pattern.compile("(http|https)://([A-Za-z0-9||_||-]*\\.)*([A-Za-z0-9||_||-]*)/");
//			Matcher m = p.matcher(values[4]);
//			if( m.find() ) {
//				domain = m.group();
//				if( domain.charAt(domain.length()-1) == '/' ) {
//					domain = domain.substring(0, domain.length()-1);
//				}
//			}
//			
//			String behavior = "24h";
//			if( domain.contains("ruten.com.tw") ) {
//				behavior = "ruten";
//			} else if( domain.contains("24h.pchome.com.tw") ) {
//				behavior = "24h";
//			} 
			
			// 1.reg待補
			// 2.走ad_click
//			if (values[13].equals("ck") && StringUtils.isNotBlank(values[15]) && values[15].matches("\\d{16}")) {
//				ACategoryLogData aCategoryLogData = categoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
//				aCategoryLogData.processCategory(values, personalInfoFactory, categoryLogBean);
//			}
			
			CategoryLogBean categoryLogBeanResult = null;
			// 露天
			if (values[13].equals("pv") && StringUtils.isNotBlank(values[4]) && values[4].contains("ruten")) {
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
				categoryLogBean.setClsfyCraspMap(clsfyCraspMap);
				categoryLogBean.setList(list);
				categoryLogBeanResult = (CategoryLogBean)aCategoryLogData.processCategory(values, categoryLogBean);
//				result2 = result.getMemid() +SYMBOL+result.getUuid()+SYMBOL+result.getAdClass()+SYMBOL+result.getAge()+SYMBOL+result.getSex();
			}

			// 24h
			if (values[13].equals("pv") && StringUtils.isNotBlank(values[4]) && values[4].contains("24h")) {
//				this.categoryLogBean = new CategoryLogBean();
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
				categoryLogBean.setList(list);
				categoryLogBean.setClsfyCraspMap(clsfyCraspMap);
				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(values, categoryLogBean);
			}
			
			if(categoryLogBeanResult == null){
				return;
			}
			
			String result = categoryLogBeanResult.getMemid() +SYMBOL+categoryLogBeanResult.getUuid()+SYMBOL+categoryLogBeanResult.getAdClass()+SYMBOL+categoryLogBeanResult.getAge()+SYMBOL+categoryLogBeanResult.getSex(); 
			keyOut.set(result);
			valueOut.set("null");
			context.write(keyOut, valueOut);
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	public static void main(String[] args) throws Exception {
		CategoryLogMapper categoryLogMapper = new CategoryLogMapper();
		categoryLogMapper.map(null, null, null);
	}

	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
}
