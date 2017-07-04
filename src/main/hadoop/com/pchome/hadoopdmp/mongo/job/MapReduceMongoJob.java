package com.pchome.hadoopdmp.mongo.job;
import java.io.IOException;
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
import com.pchome.hadoopdmp.mysql.db.service.category.IAdmCategoryAudienceAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.categoryanalyze.IAdmCategoryAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.categoryanalyze.IAdmCategoryGroupAnalyzeService;
import com.pchome.hadoopdmp.mysql.db.service.categorygroup.IAdmCategoryGroupService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
@Scope("prototype")
public class MapReduceMongoJob {
	
	private static Log log = LogFactory.getLog("MapReduceMongoJob");
	

	public static class MyMapper extends Mapper<Object, BSONObject, Text, Text> {
		
		private IAdmCategoryGroupService admCategoryGroupService;
		
		private static Map<String,String> categoryMap = new HashMap<>();
		
		private static int sum = 0;

		
		public void setup(Context context) {
			try {
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admCategoryGroupService = ctx.getBean(IAdmCategoryGroupService.class);
				
				//讀mysql大小分類表
				List<AdmCategoryGroup> admGroupList = admCategoryGroupService.loadAll();
				for (AdmCategoryGroup admCategoryGroup : admGroupList) {
					Set<AdmCategory> admCategorySet = admCategoryGroup.getAdmCategories();
					String key = "";
					List<AdmCategory> admCategoryList = new ArrayList<AdmCategory>(admCategorySet);
					int admCategorySize = admCategoryList.size();
					for (AdmCategory admCategory : admCategoryList) {
						if(admCategoryList.indexOf(admCategory) == admCategorySize - 1){
							key = key + admCategory.getAdClass();
						}else{
							key = key + admCategory.getAdClass()+"_";
						}
					}
					if(StringUtils.isNotBlank(key)){
						categoryMap.put(key, admCategoryGroup.getGroupId());//+"_TOTAL"
					}
				}
			
				log.info(">>>>>> categoryMap:"+categoryMap);
			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
			
		}
		
//		//大分類
//		2_uuid_24h_大分類代號
//		2_memid_24h_大分類代號
//		2_uuid_ruten_大分類代號
//		2_memid_ruten_大分類代號
//		2_uuid_adclick_大分類代號
//		2_memid_adclick_大分類代號
//
//
//		//男性別
//		3_uuid_24h_man
//		3_memid_24h_man
//		3_uuid_ruten_man
//		3_memid_ruten_man
//		3_uuid_adclick_man
//		3_memid_adclick_man
//
//
//		//女性別
//		3_uuid_24h_female	
//		3_memid_24h_female
//		3_uuid_ruten_female
//		3_memid_ruten_female
//		3_uuid_adclick_female
//		3_memid_adclick_female
//
//
//		//1to10年齡
//		4_uuid_24h_age01to10
//		4_memid_24h_age01to10
//		4_uuid_ruten_age01to10
//		4_memid_ruten_age01to10
//		4_uuid_adclick_age01to10
//		4_memid_adclick_age01to10
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			try {
				String category_info_str = value.get("category_info").toString().trim();
				String user_id = value.get("user_id").toString().trim();
				Map<String, Object> user_info = (Map<String, Object>) value.get("user_info");
				List<Map<String, Object>> category_info = (List<Map<String, Object>>) value.get("category_info");
				String userType = user_info.get("type").toString().trim();
				String sex = user_info.get("sex").toString().trim();
				List<Map<String, Object>> sexInfoDataList = (List<Map<String, Object>>) user_info.get("sex_info");
				String age = user_info.get("age").toString().trim();
				List<Map<String, Object>> ageInfoDataList = (List<Map<String, Object>>) user_info.get("age_info");
				Map<String, Set<String>> allMap = new HashMap<String, Set<String>>();
				String mapKey="";
				
				
				String matchSex="";
				String sexMapKey="";
				//加總男女
				if (StringUtils.isNotBlank(sex)){
					for (Map<String, Object> sexInfoObj : sexInfoDataList) {
						matchSex= sexInfoObj.get("sex").toString().trim();
						if (StringUtils.equals(sex, matchSex)){
							ArrayList<String> sourceList = (ArrayList<String>) sexInfoObj.get("source");
							for (String source : sourceList) {
								if (StringUtils.equals("ad_click", source.trim())){
									source="adclick";
								}
								sexMapKey="3_"+userType+"_"+source.trim()+"_"+sex; //性別 ex : 3_uuid_24h_M(受眾類型_會員型態_來源_性別)
								context.write(new Text(sexMapKey), new Text("1"));
								log.info(">>>>>> sexMapKey : "+sexMapKey);
							}
						}
					}
				}
				
				
				
				
				//加總性別
				String matchAge="";
				String ageMapKey="";
				String ageStr="";
				int ageInt;
				if(StringUtils.isNotBlank(age)){
					for (Map<String, Object> ageInfoObj : ageInfoDataList) {
						matchAge= ageInfoObj.get("age").toString().trim();
						if (StringUtils.equals(age, matchAge)){
							ArrayList<String> ageSourceList = (ArrayList<String>) ageInfoObj.get("source");
							for (String source : ageSourceList) {
								if (StringUtils.equals("ad_click", source.trim())){
									source="adclick";
								}
								ageInt=Integer.valueOf(age);
								if ((ageInt>=1) && (ageInt<=10)){
									ageStr="age01to10";
								}
								if ((ageInt>=11) && (ageInt<=20)){
									ageStr="age11to20";
								}
								if ((ageInt>=21) && (ageInt<=30)){
									ageStr="age21to30";
								}
								if ((ageInt>=31) && (ageInt<=40)){
									ageStr="age31to40";
								}
								if ((ageInt>=41) && (ageInt<=50)){
									ageStr="age41to50";
								}
								if ((ageInt>=51) && (ageInt<=60)){
									ageStr="age51to60";
								}
								if ((ageInt>=61) && (ageInt<=70)){
									ageStr="age61to70";
								}
								if ((ageInt>=71) && (ageInt<=80)){
									ageStr="age71to80";
								}
								if ((ageInt>=81) && (ageInt<=90)){
									ageStr="age81to90";
								}
								if ((ageInt>=91) && (ageInt<=100)){
									ageStr="age91to100";
								}
								if (ageInt>100){
									ageStr="ageover100";
								}
								ageMapKey="4_"+userType+"_"+source.trim()+"_"+ageStr; //性別 ex : 4_uuid_24h_age01to10(受眾類型_會員型態_來源_年齡)
								context.write(new Text(ageMapKey), new Text("1"));
								log.info(">>>>>> sexMapKey : "+sexMapKey);
							}
						}
					}
					
				}
				

				
				//小分類 & 大分類
				for (Map<String, Object> category : category_info) {
					String ad_class = category.get("category").toString();
					String update_date = category.get("update_date").toString();
					ArrayList<String> sourceList = (ArrayList<String>) category.get("source");
					
					if(StringUtils.isBlank(ad_class)){
						continue;
					}
					if(StringUtils.isBlank(update_date)){
						continue;
					}
					
					//process parent處理大分類
					//categoryMap : 00001_00002=0123456
					String parentCategoryMapKey="";
					for (Entry<String, String> entry : categoryMap.entrySet()) {
						if(entry.getKey().indexOf(ad_class) != -1){
							for (String source : sourceList) {
								if (StringUtils.equals("ad_click",source.trim())){
									source="adclick";
								}
								parentCategoryMapKey="2_"+userType+"_"+source.trim()+"_"+entry.getValue();//2_uuid_24h_大分類代號
								log.info(">>>>>> parentCategoryMapKey : "+parentCategoryMapKey);
								context.write(new Text(parentCategoryMapKey), new Text(user_id));
							}
							
//							log.info(">>>>>> entry.getKey():"+entry.getKey());
//							log.info(">>>>>> ad_class:"+ad_class);
//							log.info(">>>>>> entry.getValue():"+entry.getValue());
						}
					}
					
					
					
//					//處理小分類
					//1_uuid_24h_小分類代號
					String childCategoryMapKey="";
					for (String source : sourceList) {
						if (StringUtils.equals("ad_click",source.trim())){
							source="adclick";
						}
						childCategoryMapKey="1_"+userType+"_"+source.trim()+"_"+ad_class;//1_uuid_24h_小分類代號
						log.info(">>>>>> parentCategoryMapKey : "+childCategoryMapKey);
						context.write(new Text(childCategoryMapKey), new Text("1"));
					}
//					String categoryKey = ad_class + "_" + userType.toUpperCase();
//					//(000123_uuid,<"","","">
//					context.write(new Text(categoryKey), new Text("1"));
				}
				
			} catch (Exception e) {
				log.error(">>>>> Mapper e : " + e.getMessage());
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private static Set<String> data = new HashSet<>();
		
		private IAdmCategoryGroupAnalyzeService admGroupAnalyzeService;
		
		private IAdmCategoryAnalyzeService admCategoryAnalyzeService;
		
		private IAdmCategoryAudienceAnalyzeService admCategoryAudienceAnalyzeService;
		
		private static String[] mysqlColumnStr = null;
		
		
		public void setup(Context context) {
			try {
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admGroupAnalyzeService = ctx.getBean(IAdmCategoryGroupAnalyzeService.class);
				admCategoryAnalyzeService = ctx.getBean(IAdmCategoryAnalyzeService.class);
				admCategoryAudienceAnalyzeService=ctx.getBean(IAdmCategoryAudienceAnalyzeService.class);
				
			} catch (Exception e) {
				log.error(">>>>> Reducer e : " + e.getMessage());
			}
			
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				
				//性別 ex : 3_uuid_24h_M(受眾類型_會員型態_來源_性別)
				if (StringUtils.equals("3", key.toString().split("_")[0])){
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					log.info(">>>>>> sex reduce Key : "+key.toString());
					context.write(key, new Text(String.valueOf(sum)));
					//insert mysql
					mysqlColumnStr=key.toString().trim().split("_");
					AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
					admCategoryAudienceAnalyze.setRecordDate(new Date());
					admCategoryAudienceAnalyze.setKeyId(mysqlColumnStr[3]);
					admCategoryAudienceAnalyze.setKeyType(mysqlColumnStr[0]);
					admCategoryAudienceAnalyze.setUserType(mysqlColumnStr[1]);
					admCategoryAudienceAnalyze.setSource(mysqlColumnStr[2]);
					admCategoryAudienceAnalyze.setKeyCount(sum);
					admCategoryAudienceAnalyze.setCreateDate(new Date());
					admCategoryAudienceAnalyze.setUpdateDate(new Date());
					admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);	
				}
				
				
				//年齡 ex :4_uuid_24h_age01to10(受眾類型_會員型態_來源_年齡範圍)
				if (StringUtils.equals("4", key.toString().split("_")[0])){
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					log.info(">>>>>> age reduce Key : "+key.toString());
					context.write(key, new Text(String.valueOf(sum)));
					//insert mysql
					mysqlColumnStr=key.toString().trim().split("_");
					AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
					admCategoryAudienceAnalyze.setRecordDate(new Date());
					admCategoryAudienceAnalyze.setKeyId(mysqlColumnStr[3]);
					admCategoryAudienceAnalyze.setKeyType(mysqlColumnStr[0]);
					admCategoryAudienceAnalyze.setUserType(mysqlColumnStr[1]);
					admCategoryAudienceAnalyze.setSource(mysqlColumnStr[2]);
					admCategoryAudienceAnalyze.setKeyCount(sum);
					admCategoryAudienceAnalyze.setCreateDate(new Date());
					admCategoryAudienceAnalyze.setUpdateDate(new Date());
					admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);	
				}
				
				
				//大分類KEY : 2_uuid_24h_大分類代號   
				data.clear();
				if (StringUtils.equals("2", key.toString().split("_")[0])) {
					
//					String [] array = key.toString().split("_");
//					String parentKey = array[0];
//					String userType = array[2];
					
					int sum = 0;
					for (Text text : values) {
						data.add(text.toString());
						sum = sum + 1;
					}
					
//					log.info(">>>>> reduce key: " + key);
//					log.info(">>>>> reduce dataSize: " + data.size());
//					log.info(">>>>> reduce sum: " + sum);
					log.info(">>>>>> parentCategoryReduceKey : "+key.toString()+"_"+sum);
					context.write(new Text(key), new Text(String.valueOf(data.size())));
					//insert mysql
					mysqlColumnStr=key.toString().trim().split("_");
					AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
					admCategoryAudienceAnalyze.setRecordDate(new Date());
					admCategoryAudienceAnalyze.setKeyId(mysqlColumnStr[3]);
					admCategoryAudienceAnalyze.setKeyType(mysqlColumnStr[0]);
					admCategoryAudienceAnalyze.setUserType(mysqlColumnStr[1]);
					admCategoryAudienceAnalyze.setSource(mysqlColumnStr[2]);
					admCategoryAudienceAnalyze.setKeyCount(sum);
					admCategoryAudienceAnalyze.setCreateDate(new Date());
					admCategoryAudienceAnalyze.setUpdateDate(new Date());
					admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);	
					
					//insert 大分類 mysql
//					AdmCategoryGroupAnalyze admCategoryGroupAnalyze = new AdmCategoryGroupAnalyze();
//					admCategoryGroupAnalyze.setAdClassCountByHistory(data.size());
//					admCategoryGroupAnalyze.setAdGroupId(parentKey);
//					admCategoryGroupAnalyze.setUserIdType(userType);
//					admCategoryGroupAnalyze.setCreateDate(new Date());
//					admGroupAnalyzeService.save(admCategoryGroupAnalyze);					
					
				} 

				
				//處理小分類 : 1_uuid_24h_小分類代號
				if (StringUtils.equals("1", key.toString().split("_")[0])) {
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
//					log.info(">>>>> reduce key: " + key);
//					log.info(">>>>> reduce sum: " + sum);
//					log.info(">>>>> 小分類: " + key + " : " + sum);
					context.write(key, new Text(String.valueOf(sum)));
					//insert mysql
					mysqlColumnStr=key.toString().trim().split("_");
					AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
					admCategoryAudienceAnalyze.setRecordDate(new Date());
					admCategoryAudienceAnalyze.setKeyId(mysqlColumnStr[3]);
					admCategoryAudienceAnalyze.setKeyType(mysqlColumnStr[0]);
					admCategoryAudienceAnalyze.setUserType(mysqlColumnStr[1]);
					admCategoryAudienceAnalyze.setSource(mysqlColumnStr[2]);
					admCategoryAudienceAnalyze.setKeyCount(sum);
					admCategoryAudienceAnalyze.setCreateDate(new Date());
					admCategoryAudienceAnalyze.setUpdateDate(new Date());
					admCategoryAudienceAnalyzeService.save(admCategoryAudienceAnalyze);	
					
					//insert 小分類 mysql
//					AdmCategoryAnalyze admCategoryAnalyze = new AdmCategoryAnalyze();
//					admCategoryAnalyze.setRecodeDate(new Date());
//					admCategoryAnalyze.setAdClass(key.toString().split("_")[0]);
//					admCategoryAnalyze.setUserIdType(key.toString().split("_")[1]);
//					admCategoryAnalyze.setAdClassCountByDay(sum);
//					admCategoryAnalyze.setCreateDate(new Date());
//					admCategoryAnalyze.setUdpateDate(new Date());
//					admCategoryAnalyze.setSexManCount(0);
//					admCategoryAnalyze.setSexWomanCount(0);
//					admCategoryAnalyze.setAgeRangeCount1to10(0);
//					admCategoryAnalyze.setAgeRangeCount11to20(0);
//					admCategoryAnalyze.setAgeRangeCount21to30(0);
//					admCategoryAnalyze.setAgeRangeCount31to40(0);
//					admCategoryAnalyze.setAgeRangeCount41to50(0);
//					admCategoryAnalyze.setAgeRangeCount51to60(0);
//					admCategoryAnalyze.setAgeRangeCount61to70(0);
//					admCategoryAnalyze.setAgeRangeCount71to80(0);
//					admCategoryAnalyze.setAgeRangeCount81to90(0);
//					admCategoryAnalyze.setAgeRangeCount91to100(0);
//					admCategoryAnalyzeService.save(admCategoryAnalyze);				
				}
			} catch (Exception e) {
				log.error(">>>>> Reducer e : " + e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.class_count_test");
		// conf.set("mongo.input.query",
		// "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
		// conf.set("mongo.input.query", "{'update_date':{'$gt':'2017-06-19
		// 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);

		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "alex_test");
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
//		new MyMapper().setup(null);
	}
}