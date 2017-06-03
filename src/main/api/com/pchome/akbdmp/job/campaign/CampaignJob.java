package com.pchome.akbdmp.job.campaign;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.mongo.db.service.classcount.IClassCountService;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;

@Component
public class CampaignJob {

	Log log = LogFactory.getLog("AkbDmpJob");

	@Value("${save.path.campaignlog}")
	private String campaignlogPath;

	@Autowired
	private IClassCountService classCountService;
	
	public void process() throws Exception {
		log.info("====CampaignJob.process() start====");

		log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>" + campaignlogPath);

		File dir = new File(campaignlogPath);
		if (!dir.exists()) {
			log.error(dir.getPath() + " not exists");
			return;
		}

		String line = null;
		String[] lines = null;
		String memid = null;
		String uuid = null;
		String adClass = null;
		int count = 0;
		String age = null;
		String sex = null;
		String ipArea = null;
		String recordDate = null;
		boolean overWrite = false;

		FilenameFilter filenameFilter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return !name.endsWith(".err");
			}
		};

		for (File file : dir.listFiles(filenameFilter)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				while ((line = br.readLine()) != null) {
					lines = line.split(",");
					if (lines.length < 9) {
						continue;
					}

					memid = lines[0];
					uuid = lines[1];
					adClass = lines[2];
					count = Integer.parseInt(lines[3]);
					age = lines[4];
					sex = lines[5];
					ipArea = lines[6];
					recordDate = lines[7];
					overWrite = Boolean.parseBoolean(lines[8]);
					
					
					ClassCountMongoBean classCountMongoBean = new ClassCountMongoBean();
					classCountMongoBean.setMemid(memid);
					classCountMongoBean.setUuid(uuid);
					classCountMongoBean.setBehavior("campaign");
					classCountMongoBean.setAd_class(adClass);
					classCountMongoBean.setCount(count);
					classCountMongoBean.setRecord_date(recordDate);
					classCountService.saveOrUpdate(classCountMongoBean);
					
//					classCountService.saveOrUpdate(memid, uuid, BEHAVIOR, adClass, count, recordDate, overWrite);
//					personalInformationService.saveOrUpdate(memid, uuid, age, sex, ipArea, overWrite);
				}

//				log.info("delete " + file);
//				FileUtils.deleteQuietly(file);
//			} catch (Exception e) {
//				log.error(file, e);
////				File errFile = new File(file.getPath() + ".err");
////				try {
////					log.info("mv " + errFile);
////					FileUtils.moveFile(file, errFile);
////				} catch (IOException ioe) {
////					log.error(errFile, ioe);
////				}
//			}
		}

		log.info("====CampaignJob.process() end====");
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		try {
			System.setProperty("spring.profiles.active", "stg");
			ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
			CampaignJob campaignJob = ctx.getBean(CampaignJob.class);
			campaignJob.process();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
