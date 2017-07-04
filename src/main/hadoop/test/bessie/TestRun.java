package test.bessie;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategory;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.hadoopdmp.mysql.db.service.category.IAdmCategoryService;
import com.pchome.hadoopdmp.mysql.db.service.categorygroup.IAdmCategoryGroupService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
import com.pchome.soft.util.DateFormatUtil;

@Component
public class TestRun {
	
	Log log = LogFactory.getLog(TestRun.class);
	
	@Autowired
	private DateFormatUtil dateFormatUtil;
	
	@Autowired
	private IAdmCategoryGroupService admCategoryGroupService;
	
	@Autowired
	private IAdmCategoryService admCategoryService;
	
	
	
//	//查詢
	private void hibernateDbTest(){
		List<AdmCategoryGroup> admAdGroupList = admCategoryGroupService.loadAll();
		System.out.println("Group size:"+admAdGroupList.size());
		for (AdmCategoryGroup admAdGroup : admAdGroupList) {
			System.out.println("Group name:"+admAdGroup.getGroupName());
			Set<AdmCategory> admAdClassSet = admAdGroup.getAdmCategories();
			for (AdmCategory admAdClass : admAdClassSet) {
				System.out.println(admAdClass.getAdClassName());
			}
			
		}
	}
	
	
	
	
	//新增大類別
	private void hibernateDbTest2() throws Exception{
		Date date = new Date();
		String dateStr = dateFormatUtil.getDateTemplate2().format(date);
		date = dateFormatUtil.getDateTemplate2().parse(dateStr);
		
		AdmCategoryGroup admCategoryGroup = new AdmCategoryGroup();
		admCategoryGroup.setGroupName("運動類");
		admCategoryGroup.setGroupId("0000000000000003");
		admCategoryGroup.setCreateDate(date);
		admCategoryGroup.setUpdateDate(date);
		admCategoryGroupService.save(admCategoryGroup);
		
	}
	
	//新增小類別
	private void hibernateDbTest3() throws Exception{
		AdmCategoryGroup admCategoryGroup = admCategoryGroupService.get(2);
		System.out.println(admCategoryGroup.getGroupId());
		
		Date date = new Date();
		String dateStr = dateFormatUtil.getDateTemplate2().format(date);
		date = dateFormatUtil.getDateTemplate2().parse(dateStr);
		
		AdmCategory admCategory = new AdmCategory();
		admCategory.setAdClass("AAAAAAAAAAAAAAAA");
		admCategory.setAdClassName("測試用");
		admCategory.setAdmCategoryGroup(admCategoryGroup);
		admCategory.setCreateDate(date);
		admCategory.setUpdateDate(date);
		admCategoryService.save(admCategory);
	}
	
	
	private void split(){
//		String str="0001004706430000_UUID";
//		String adclass=str.split("_")[0];
//		String type=str.split("_")[1];
//		
//		System.out.println(adclass);
//		System.out.println(type);
		
//		String str1="2017-06-20 16:04:40";
//		System.out.println("str1: "+str1.split(" ")[0]);
		
		
//		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//		Date date = new Date();
//		String today=dateFormat.format(date);
//		System.out.println(today);
		
		
		String age="01";
		int ageInt=Integer.valueOf(age);
		System.out.println("ageInt: "+ageInt);
		
		if ((ageInt>=1) && (ageInt<=10)){
			System.out.println("age01to10");
		}
		
//		System.out.println(!StringUtils.equals("123", "124"));
		
	}
	
	// List指標
	private void listTest(){
		List<String> list = new ArrayList<>();
		list.add("a01");
		list.add("a02");
		list.add("a03");

		for (String string : list) {
			System.out.println(string);
			System.out.println(list.indexOf(string));
		}
		
	}
	
	
	
	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "local");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
			TestRun TestRun = (TestRun) ctx.getBean(TestRun.class);
//			TestRun.hibernateDbTest();
//			TestRun.listTest();
//			TestRun.hibernateDbTest2();
//			TestRun.hibernateDbTest3();
//			TestRun.split();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

	}
}
