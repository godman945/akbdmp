package test.bessie;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
	
	//查詢
//	private void hibernateDbTest(){
//		List<AdmAdGroup> admAdGroupList = admAdGroupService.loadAll();
//		for (AdmAdGroup admAdGroup : admAdGroupList) {
//			System.out.println("Group name:"+admAdGroup.getName());
//			Set<AdmAdClass> admAdClassSet = admAdGroup.getAdmAdClasses();
//			for (AdmAdClass admAdClass : admAdClassSet) {
//				System.out.println(admAdClass.getName());
//			}
//			
//		}
//	}
	
	
	
	
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
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
