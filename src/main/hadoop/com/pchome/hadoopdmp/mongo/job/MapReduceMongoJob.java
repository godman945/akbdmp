package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategory;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryAudienceAnalyze;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.hadoopdmp.enumerate.CategoryComparisonTableEnum;
import com.pchome.hadoopdmp.mysql.db.service.category.IAdmCategoryAudienceAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.categorygroup.IAdmCategoryGroupService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
@Scope("prototype")
public class MapReduceMongoJob {

	private static Log log = LogFactory.getLog("MapReduceMongoJob");

	
	
	public static class MyMapper extends Mapper<Object, BSONObject, Text, Text> {

		private IAdmCategoryGroupService admCategoryGroupService;

		private static Map<String, String> categoryMap = new HashMap<>();

		public void setup(Context context) {
			try {
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admCategoryGroupService = ctx.getBean(IAdmCategoryGroupService.class);

				// 讀mysql大小分類表放入categoryMap
				List<AdmCategoryGroup> admGroupList = admCategoryGroupService.loadAll();
				for (AdmCategoryGroup admCategoryGroup : admGroupList) {
					Set<AdmCategory> admCategorySet = admCategoryGroup.getAdmCategories();
					String key = "";
					List<AdmCategory> admCategoryList = new ArrayList<AdmCategory>(admCategorySet);
					int admCategorySize = admCategoryList.size();
					for (AdmCategory admCategory : admCategoryList) {
						if (admCategoryList.indexOf(admCategory) == admCategorySize - 1) {
							key = key + admCategory.getAdClass();
						} else {
							key = key + admCategory.getAdClass() + "_";
						}
					}
					if (StringUtils.isNotBlank(key)) {
						categoryMap.put(key, admCategoryGroup.getGroupId());// 大小分類對照表categoryMap(小分類_小分類 = 大分類) : 6010000000001_6010000000002 = 100000000000000
					}
				}

				log.info(">>>>>> map setup categoryMap:" + categoryMap);
				
				//output每日大小分類對照表至hdfs
				context.write(new Text("Daily_Category_Comparison_Table"), new Text(categoryMap.toString()));
			} catch (Exception e) {
				log.error(">>>>> mapper setup exception : " + e.getMessage());
			}

		}

		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			try {
				String category_info_str = value.get("category_info").toString().trim();
				String user_id = value.get("user_id").toString().trim();
				Map<String, Object> user_info = (Map<String, Object>) value.get("user_info");
				List<Map<String, Object>> category_info = (List<Map<String, Object>>) value.get("category_info");
				String userType = user_info.get("type").toString().trim();
				String sex = user_info.get("sex").toString().toUpperCase().trim();
				List<Map<String, Object>> sexInfoDataList = (List<Map<String, Object>>) user_info.get("sex_info");
				String age = user_info.get("age").toString().trim();
				List<Map<String, Object>> ageInfoDataList = (List<Map<String, Object>>) user_info.get("age_info");
				

				// 計算每個來源的性別加總
				String matchSex = "";
				String sexMapKey = "";
				if (StringUtils.isNotBlank(sex)) {
					for (Map<String, Object> sexInfoObj : sexInfoDataList) {
						matchSex = sexInfoObj.get("sex").toString().trim();
						if (StringUtils.equals(sex, matchSex)) {
							ArrayList<String> sexSourceList = (ArrayList<String>) sexInfoObj.get("source");
							for (String source : sexSourceList) {
								if (StringUtils.equals("ad_click", source.trim())) {
									source = "adclick";
								}
								sexMapKey = "3_" + userType.toLowerCase().trim() + "_" + source.toLowerCase().trim() + "_" + sex; // 性別 ex : 3_uuid_24h_M(受眾類型_會員型態_來源_性別)
								context.write(new Text(sexMapKey), new Text("1"));
//								log.info(">>>>>> sexMapKey : " + sexMapKey);
							}
						}
					}
				}

				
				// 計算每個來源的年齡加總
				String matchAge = "";
				String ageMapKey = "";
				String ageStr = "";
				int ageInt;
				if (StringUtils.isNotBlank(age)) {
					for (Map<String, Object> ageInfoObj : ageInfoDataList) {
						matchAge = ageInfoObj.get("age").toString().trim();
						if (StringUtils.equals(age, matchAge)) {
							ArrayList<String> ageSourceList = (ArrayList<String>) ageInfoObj.get("source");
							for (String source : ageSourceList) {
								if (StringUtils.equals("ad_click", source.trim())) {
									source = "adclick";
								}
								ageInt = Integer.valueOf(age);
								
								if ((ageInt >= 1) && (ageInt <= 10)) {
									ageStr = "age01to10";
								}
								if ((ageInt >= 11) && (ageInt <= 20)) {
									ageStr = "age11to20";
								}
								if ((ageInt >= 21) && (ageInt <= 30)) {
									ageStr = "age21to30";
								}
								if ((ageInt >= 31) && (ageInt <= 40)) {
									ageStr = "age31to40";
								}
								if ((ageInt >= 41) && (ageInt <= 50)) {
									ageStr = "age41to50";
								}
								if ((ageInt >= 51) && (ageInt <= 60)) {
									ageStr = "age51to60";
								}
								if ((ageInt >= 61) && (ageInt <= 70)) {
									ageStr = "age61to70";
								}
								if ((ageInt >= 71) && (ageInt <= 80)) {
									ageStr = "age71to80";
								}
								if ((ageInt >= 81) && (ageInt <= 90)) {
									ageStr = "age81to90";
								}
								if ((ageInt >= 91) && (ageInt <= 100)) {
									ageStr = "age91to100";
								}
								if (ageInt > 100) {
									ageStr = "ageover100";
								}
								
								ageMapKey = "4_" + userType.toLowerCase().trim() + "_" + source.toLowerCase().trim() + "_" + ageStr; // 年齡 ex : 4_uuid_24h_age01to10(受眾類型_會員型態_來源_年齡範圍)
								context.write(new Text(ageMapKey), new Text("1"));
//								log.info(">>>>>> ageMapKey : " + ageMapKey);
							}
						}
					}
				}

				
				// 計算每個來源小分類  & 大分類的加總
				for (Map<String, Object> category : category_info) {
					String ad_class = category.get("category").toString();
					ArrayList<String> sourceList = (ArrayList<String>) category.get("source");

					if (StringUtils.isBlank(ad_class)) {
						continue;
					}

					// 處理大分類			// 大小分類對照表categoryMap(小分類_小分類 = 大分類) : 6010000000001_6010000000002 = 100000000000000
					String parentCategoryMapKey = "";
					for (Entry<String, String> entry : categoryMap.entrySet()) {
						if (entry.getKey().indexOf(ad_class) != -1) {
							for (String source : sourceList) {
								if (StringUtils.equals("ad_click", source.trim())) {
									source = "adclick";
								}
								parentCategoryMapKey = "2_" + userType.toLowerCase().trim() + "_" + source.toLowerCase().trim() + "_" + entry.getValue();// 2_uuid_24h_大分類代號
//								log.info(">>>>>> parentCategoryMapKey : " + parentCategoryMapKey);
								context.write(new Text(parentCategoryMapKey), new Text(user_id));
							}
						}
					}

					// 處理小分類 
					String childCategoryMapKey = "";
					for (String source : sourceList) {
						if (StringUtils.equals("ad_click", source.trim())) {
							source = "adclick";
						}
						childCategoryMapKey = "1_" + userType.toLowerCase().trim() + "_" + source.toLowerCase().trim() + "_" + ad_class;// 1_uuid_24h_小分類代號
//						log.info(">>>>>> childCategoryMapKey : " + childCategoryMapKey);
						context.write(new Text(childCategoryMapKey), new Text("1"));
					}
				}
			} catch (Exception e) {
				log.error(">>>>> Mapper exception : " + e.getMessage());
			}
		}
	}

	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		private static Set<String> data = new HashSet<>();

		private IAdmCategoryAudienceAnalyzeService admCategoryAudienceAnalyzeService;

		private static String[] mysqlColumnStr = null;

		private static String[] reduceKeyArray = null;

		public void setup(Context context) {
			try {
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admCategoryAudienceAnalyzeService = ctx.getBean(IAdmCategoryAudienceAnalyzeService.class);
			} catch (Exception e) {
				log.error(">>>>> Reducer setup exception : " + e.getMessage());
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {

				// 輸出每日大小分類對照表
				if (StringUtils.equals("Daily_Category_Comparison_Table", key.toString().trim())) {
					String categoryMap = "";
					for (Text text : values) {
						categoryMap = text.toString();
					}
					log.info(">>>>>> reduce categoryMap : " + categoryMap.toString());
					context.write(new Text(categoryMap), new Text(""));
				}
				

				// 依照key找enum的name
				String reduceKeyStr = key.toString().trim();
				String keyId = reduceKeyStr.split("_")[3].trim();
				String keyName = "";
				for (CategoryComparisonTableEnum item : CategoryComparisonTableEnum.values()) {
					if (StringUtils.equals(keyId, item.getKey().trim())) {
						keyName = item.getName();
						break;

					} else {
						keyName = "null";
					}
				}
				reduceKeyStr = reduceKeyStr + "_" + keyName.trim();
				reduceKeyArray = reduceKeyStr.split("_");
				String key_type = reduceKeyArray[0].trim();// 受眾類型   1:小分類 2:大分類 3:性別 4:年齡
				
				// 大分類KEY : 2_uuid_24h_大分類代號_媽媽族(算聯集)(受眾類型_會員型態_來源_大分類代號_大分類name)
				data.clear();
				if (StringUtils.equals("2", key_type)) {
					int sum = 0;
					for (Text text : values) {
						data.add(text.toString());
					}
					sum = data.size();
					context.write(new Text(reduceKeyStr), new Text(String.valueOf(sum)));
					insertMysqlAudienceTable(reduceKeyStr.toString(), sum);
				}

				// 小分類KEY : 1_uuid_24h_小分類代號_口紅(受眾類型_會員型態_來源_小分類代號_小分類name)
				if (StringUtils.equals("1", key_type)) {
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					context.write(new Text(reduceKeyStr), new Text(String.valueOf(sum)));
					insertMysqlAudienceTable(reduceKeyStr.toString(), sum);
				}
				
				// 性別KEY : 3_uuid_24h_M_男(受眾類型_會員型態_來源_性別_key的中文name)
				if (StringUtils.equals("3", key_type)) {
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					context.write(new Text(reduceKeyStr), new Text(String.valueOf(sum)));
					insertMysqlAudienceTable(reduceKeyStr.toString(), sum);

				}

				// 年齡 KEY :4_uuid_24h_age01to10_年齡1~10(受眾類型_會員型態_來源_年齡範圍_年齡name)
				if (StringUtils.equals("4", key_type)) {
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					context.write(new Text(reduceKeyStr), new Text(String.valueOf(sum)));
					insertMysqlAudienceTable(reduceKeyStr.toString(), sum);
				}
			} catch (Exception e) {
				log.error(">>>>> Reducer exception : " + e.getMessage());
			}
		}

		public void insertMysqlAudienceTable(String key, int sum) {
			mysqlColumnStr = key.toString().split("_");
			AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
			admCategoryAudienceAnalyze.setRecordDate(new Date());
			admCategoryAudienceAnalyze.setKeyId(mysqlColumnStr[3]);
			admCategoryAudienceAnalyze.setKeyName(mysqlColumnStr[4]);
			admCategoryAudienceAnalyze.setKeyType(mysqlColumnStr[0]);
			admCategoryAudienceAnalyze.setUserType(mysqlColumnStr[1]);
			admCategoryAudienceAnalyze.setSource(mysqlColumnStr[2]);
			admCategoryAudienceAnalyze.setKeyCount(sum);
			admCategoryAudienceAnalyze.setCreateDate(new Date());
			admCategoryAudienceAnalyze.setUpdateDate(new Date());
			admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);
		}

	}

	public static void main(String[] args) throws Exception {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		IAdmCategoryAudienceAnalyzeService admCategoryAudienceAnalyzeService = ctx.getBean(IAdmCategoryAudienceAnalyzeService.class);

//		List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.loadAll();
//		System.out.println("all size: "+list.size());
//		
//		AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
//		admCategoryAudienceAnalyze.setRecordDate(new Date());
//		admCategoryAudienceAnalyze.setKeyId("test");
//		admCategoryAudienceAnalyze.setKeyName("3C");
//		admCategoryAudienceAnalyze.setKeyType("uuid");
//		admCategoryAudienceAnalyze.setUserType("uuid");
//		admCategoryAudienceAnalyze.setSource("24h");
//		admCategoryAudienceAnalyze.setKeyCount(100);
//		admCategoryAudienceAnalyze.setCreateDate(new Date());
//		admCategoryAudienceAnalyze.setUpdateDate(new Date());
//		admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);
//		
//		List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.loadAll();
//		System.out.println("add size: "+list.size());
//		
		
		
		
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date current = new Date();
		String date = sdFormat.format(current);
	    Date today = sdFormat.parse(date);;
//	    
	    //hibernate delete mysql today all record 
		String query = " from AdmCategoryAudienceAnalyze where recordDate = ? ";
	    Object[] queryParam = {today};//2017-07-05
	    List<AdmCategoryAudienceAnalyze> todayRecordList= (List<AdmCategoryAudienceAnalyze>)admCategoryAudienceAnalyzeService.findHql(query, queryParam);
	    
	    log.info(">>>>>> hibername delete all before : " + todayRecordList.size());
	    admCategoryAudienceAnalyzeService.deleteAll(todayRecordList);
	    List<AdmCategoryAudienceAnalyze> allList=admCategoryAudienceAnalyzeService.loadAll();
	    log.info(">>>>>> hibername delete all List size after : " + allList.size());
		
	    
	    
	    
//
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.class_count");
		// conf.set("mongo.input.query",
		// "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
		// conf.set("mongo.input.query", "{'update_date':{'$gt':'2017-06-19
		// 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);

		final Job job = new Job(conf, "AkbDmp_Category_Audience_Analyze_" + date);
		Path out = new Path("/home/webuser/alex/mongo");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(MapReduceMongoJob.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}