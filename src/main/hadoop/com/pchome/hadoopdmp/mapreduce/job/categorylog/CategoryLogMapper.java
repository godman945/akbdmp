package com.pchome.hadoopdmp.mapreduce.job.categorylog;

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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.enumerate.CategoryLogEnum;
import com.pchome.hadoopdmp.mapreduce.job.component.PersonalInfoComponent;
import com.pchome.hadoopdmp.mapreduce.job.factory.ACategoryLogData;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryCodeBean;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogBean;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryLogFactory;
import com.pchome.hadoopdmp.mapreduce.job.factory.CategoryRutenCodeBean;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.hadoopdmp.spring.config.bean.mongodb.MongodbHadoopConfig;

@Component
public class CategoryLogMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("CategoryLogMapper");
	
	private static int LOG_LENGTH = 30;
	private static String SYMBOL = String.valueOf(new char[] { 9, 31 });

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;
	public static ArrayList<Map<String, String>> categoryList = new ArrayList<Map<String, String>>();		     //分類表	
	public static Map<String, combinedValue> clsfyCraspMap = new HashMap<String, combinedValue>();				 //分類個資表
	public static List<CategoryCodeBean> category24hBeanList = new ArrayList<CategoryCodeBean>();				 //24H分類表
	public static List<CategoryRutenCodeBean> categoryRutenBeanList = new ArrayList<CategoryRutenCodeBean>();	 //Ruten分類表
	public static PersonalInfoComponent personalInfoComponent = new PersonalInfoComponent();
	private MongoOperations mongoOperations;
	

	private int adClick_process = 0;
	private int tweenFour_process = 0;
	private int ruten_process = 0;
	private long time1, time2,time3;
	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		time1 = System.currentTimeMillis();
		try {
			if(StringUtils.isNotBlank(context.getConfiguration().get("spring.profiles.active"))){
				System.setProperty("spring.profiles.active", context.getConfiguration().get("spring.profiles.active"));
			}else{
				System.setProperty("spring.profiles.active", "stg");
			}
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			this.mongoOperations = ctx.getBean(MongodbHadoopConfig.class).mongoProducer();
			record_date = context.getConfiguration().get("job.date");
			Configuration conf = context.getConfiguration();
			
			//load 分類個資表(ClsfyGndAgeCrspTable.txt)
			org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
			Path clsfyTable = Paths.get(path[1].toString());
			Charset charset = Charset.forName("UTF-8");
			List<String> lines = Files.readAllLines(clsfyTable, charset);
			for (String line : lines) {
				String[] tmpStrAry = line.split(";"); // 0001000000000000;M,35
				String[] tmpStrAry2 = tmpStrAry[1].split(",");
				clsfyCraspMap.put(tmpStrAry[0],new combinedValue(tmpStrAry[1].split(",")[0], tmpStrAry2.length > 1 ? tmpStrAry2[1] : ""));
			}
			

			// load 分類表(pfp_ad_category_new.csv)
			Path cate_path = Paths.get(path[0].toString());
			charset = Charset.forName("UTF-8");
			int maxCateLvl = 4;
			categoryList = new ArrayList<Map<String, String>>();
			for (int i = 0; i < maxCateLvl; i++) {
				categoryList.add(new HashMap<String, String>());
			}
			lines.clear();
			lines = Files.readAllLines(cate_path, charset);
			// 將 table: pfp_ad_category_new 內容放入list中(共有 maxCateLvl 層)
			for (String line : lines) {
				String[] tmpStr = line.split(";");
				int lvl = Integer.parseInt(tmpStr[5].replaceAll("\"", "").trim());
				if (lvl <= maxCateLvl) {
					categoryList.get(lvl - 1).put(tmpStr[3].replaceAll("\"", "").trim(),tmpStr[4].replaceAll("\"", "").replaceAll("@", "").trim());
				}
			}
			
			// load 24h分類表(DMP_24h_category.csv)
			Path category24HPath = Paths.get(path[3].toString());
			List<String> lines24H = Files.readAllLines(category24HPath, charset);
			for (String line : lines24H) {
				CategoryCodeBean categoryBean = new CategoryCodeBean();
				
				String[] tmpStrAry = line.split(","); // 0001000000000000;M,35
				
				categoryBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
				categoryBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
				categoryBean.setBreadCrumb(tmpStrAry[2].replaceAll("\"", ""));
				categoryBean.setEnglishCode(tmpStrAry[3].replaceAll("\"", ""));
				
				category24hBeanList.add(categoryBean);
			}
			
			// load Ruten分類表(DMP_Ruten_category.csv)
			Path categoryRutenPath = Paths.get(path[4].toString());
			List<String> linesRuten = Files.readAllLines(categoryRutenPath, charset);
			for (String line : linesRuten) {
				CategoryRutenCodeBean categoryRutenBean = new CategoryRutenCodeBean();
				
				String[] tmpStrAry = line.split(","); //"0001000000000000","電腦、電腦周邊"
				
				categoryRutenBean.setNumberCode(tmpStrAry[0].replaceAll("\"", ""));
				categoryRutenBean.setChineseDesc(tmpStrAry[1].replaceAll("\"", ""));
				
				categoryRutenBeanList.add(categoryRutenBean);
			}
			
			
		} catch (Exception e) {
			log.info("Mapper  setup Exception: "+e.getMessage());
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		
		/*Kdcl log	raw data格式
		// values[1] //memid
		// values[2] //uuid
		// values[4] //url
		// values[13] //ck,pv
		// values[15] //ad_class
		 */
		
		/*Campaign log raw data格式
		// values[0] //memid		會員帳號
		// values[1] //uuid			通用唯一識別碼	
		// values[2] //ad_class		分類
		// values[3] //Count		數量
		// values[4] //age			年齡
		// values[5] //sex			性別(F|M)
		// values[6] //ip_area		地區
		// values[7] //record_date	紀錄日期(yyyy-MM-dd)
		// values[8] //Over_write	是否覆寫(true|false)
		 */
		try {
			
//			log.info("raw_data [memid] : " + values[1]);
//			log.info("raw_data [uuid] : " + values[2]);
//			log.info("raw_data [url] : " + values[4]);
//			log.info("raw_data [ck,pv] : " + values[13]);
//			log.info("raw_data [ad_class] : " + values[15]);
//			String[] values = value.toString().split(SYMBOL);
//			if (values.length < LOG_LENGTH) {
//				log.info("values.length < " + LOG_LENGTH);
//				return;
//			}
			
			CategoryLogBean categoryRawDataBean = new CategoryLogBean();
			String valuestr = value.toString();
			//kdcl log
			if ( valuestr.indexOf(SYMBOL) >-1){
				String[] values = valuestr.toString().split(SYMBOL);
				
				if (values.length < LOG_LENGTH) {
					log.info("values.length < " + LOG_LENGTH);
					return;
				}
				categoryRawDataBean.setMemid(values[1]);
				categoryRawDataBean.setUuid(values[2]);
				categoryRawDataBean.setUrl(values[4]);
				categoryRawDataBean.setAdClass(values[15]);
				categoryRawDataBean.setSource(values[13]);
				
				log.info(">>>>>> kdcl rawdata:" + valuestr);
				
			}else{	//campaign log
				String[] values = valuestr.toString().split(",");
				
				 if (values.length < 9) {
					 return;
                 }
				 categoryRawDataBean.setMemid(values[0]);
				 categoryRawDataBean.setUuid(values[1]);
				 categoryRawDataBean.setUrl("");
				 categoryRawDataBean.setAdClass(values[2]);
				 categoryRawDataBean.setSource("campaign");
				 
				 log.info(">>>>>> campaige rawdata:" + valuestr);
			}
			

			CategoryLogBean categoryLogBean = new CategoryLogBean();
			CategoryLogBean categoryLogBeanResult = null;
			
			//分析click、24H、Ruten 分類 
			if ( (categoryRawDataBean.getSource().equals("ck")||categoryRawDataBean.getSource().equals("campaign")) && StringUtils.isNotBlank(categoryRawDataBean.getAdClass())) {	// kdcl log的ad_click 或 campaign log的adclass 
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.AD_CLICK);
				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(categoryRawDataBean, categoryLogBean, mongoOperations);
			}else if (categoryRawDataBean.getSource().equals("pv") && StringUtils.isNotBlank(categoryRawDataBean.getUrl()) && categoryRawDataBean.getUrl().contains("ruten")) {	// 露天
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_RETUN);
				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(categoryRawDataBean, categoryLogBean, mongoOperations);
			}else if (categoryRawDataBean.getSource().equals("pv") && StringUtils.isNotBlank(categoryRawDataBean.getUrl()) && categoryRawDataBean.getUrl().contains("24h")) {		// 24h
				ACategoryLogData aCategoryLogData = CategoryLogFactory.getACategoryLogObj(CategoryLogEnum.PV_24H);
				categoryLogBeanResult = (CategoryLogBean) aCategoryLogData.processCategory(categoryRawDataBean, categoryLogBean, mongoOperations);
			}else{
				return;
			}
			
//			//分類物件為null，不往下做
//			if (categoryLogBeanResult == null) {
//				return;
//			}
			
			//處理個資
			categoryLogBeanResult = personalInfoComponent.processPersonalInfo(categoryLogBeanResult, mongoOperations);
							
			
			categoryLogBeanResult.setRecodeDate(record_date);
			// 0:Memid + 1:Uuid + 2:AdClass + 3.URL +
			// 4.Source + 5.MSex(會員中心性別) + 6.MAge(會員中心年齡) 
			// 7.PersonalInfoMemberApiClassify(打會員api是否有完整個資 Y/N) + 8.Sex(推估性別) + 9.Age(推估年齡) +
			// 10.PersonalInfoClassify(依adClass比對分類年齡性別對應表ClsfyGndAgeCrspTable，是否有完整的推估個資 Y/N)
			String memid = StringUtils.isBlank(categoryLogBeanResult.getMemid()) ? "null" : categoryLogBeanResult.getMemid();
			String result = memid + SYMBOL + categoryLogBeanResult.getUuid() + SYMBOL + categoryLogBeanResult.getAdClass() + SYMBOL  + categoryRawDataBean.getUrl() 
							+ SYMBOL + categoryLogBeanResult.getSource() + SYMBOL + categoryLogBeanResult.getMsex() + SYMBOL + categoryLogBeanResult.getMage()  
							+ SYMBOL + categoryLogBeanResult.getPersonalInfoMemberApiClassify()+ SYMBOL + categoryLogBeanResult.getSex() + SYMBOL + categoryLogBeanResult.getAge() + SYMBOL 
							+ categoryLogBeanResult.getPersonalInfoClassify() ;
			
			log.info(">>>>>> Mapper write key:" + result);
			
			
			/*舊版			
			 0:Memid + 1:Uuid + 2:AdClass + 3:Age + 4:Sex + 5:Source + 6:RecodeDate + 7:Type + 8:Classify
			String result = memid + SYMBOL + categoryLogBeanResult.getUuid() + SYMBOL + categoryLogBeanResult.getAdClass() + SYMBOL + categoryLogBeanResult.getAge() + SYMBOL + categoryLogBeanResult.getSex() + SYMBOL + categoryLogBeanResult.getSource()+ SYMBOL +categoryLogBeanResult.getRecodeDate() + SYMBOL + categoryLogBeanResult.getType() + SYMBOL + values[4] + SYMBOL + categoryLogBeanResult.getType()+"_"+categoryLogBeanResult.getSource()+"_"+categoryLogBeanResult.getBehaviorClassify() + SYMBOL + "user_info_Classify_"+categoryLogBeanResult.getPersonalInfoClassify();
			 */	
			
			keyOut.set(result);
			context.write(keyOut, valueOut);
			
		} catch (Exception e) {
			log.error(">>>>>> " + e.getMessage());
		}

	}
	

//	 @Autowired
//	 MongoOperations mongoOperations;
	 public void test() throws Exception{
//	 System.out.println("BBB");
//	 Query query = new
//	 Query(Criteria.where("_id").is("59404c00e4b0ed734829caf4"));
//	 ClassUrlMongoBean classUrlMongoBean = (ClassUrlMongoBean)
//	 mongoOperations.findOne(query, ClassUrlMongoBean.class);
//	 System.out.println(classUrlMongoBean.getUrl());
//	 System.out.println("AAA");
	 }

	public static void main(String[] args) throws Exception {
//		 CategoryLogMapper categoryLogMapper = new CategoryLogMapper();
//		 categoryLogMapper.map(null, null, null);

//		System.setProperty("spring.profiles.active", "stg");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		CategoryLogMapper categoryLogMapper = ctx.getBean(CategoryLogMapper.class);
//		categoryLogMapper.test();
//		 categoryLogMapper.map(null, null, null);

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
