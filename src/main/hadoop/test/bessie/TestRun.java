package test.bessie;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.data.mysql.pojo.AdmAdClass;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmAdGroup;
import com.pchome.hadoopdmp.mysql.db.service.ad.IAdmAdGroupService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class TestRun {
	
	Log log = LogFactory.getLog(TestRun.class);
	
	@Autowired
	IAdmAdGroupService admAdGroupService;
	
	private void hibernateDbTest(){
		log.info(">>>>>>>>>>>");
		
		List<AdmAdGroup> admAdGroupList = admAdGroupService.loadAll();
		for (AdmAdGroup admAdGroup : admAdGroupList) {
			System.out.println("Group name:"+admAdGroup.getName());
			Set<AdmAdClass> admAdClassSet = admAdGroup.getAdmAdClasses();
			for (AdmAdClass admAdClass : admAdClassSet) {
				System.out.println(admAdClass.getName());
			}
			
		}
		
	}
	
	public static void main(String[] args) {
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		TestRun TestRun = (TestRun) ctx.getBean(TestRun.class);
		TestRun.hibernateDbTest();
	}
}
