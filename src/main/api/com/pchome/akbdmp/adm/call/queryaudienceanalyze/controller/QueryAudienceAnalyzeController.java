package com.pchome.akbdmp.adm.call.queryaudienceanalyze.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.pchome.akbdmp.api.call.base.controller.BaseController;
import com.pchome.akbdmp.data.mysql.pojo.AdmCategoryAudienceAnalyze;
import com.pchome.akbdmp.mysql.db.service.adclass.IAdmCategoryAudienceAnalyzeService;

import net.minidev.json.JSONObject;

@RestController
@Scope("request")
public class QueryAudienceAnalyzeController extends BaseController {

	Log log = LogFactory.getLog(QueryAudienceAnalyzeController.class);

	@Autowired
	private IAdmCategoryAudienceAnalyzeService admCategoryAudienceAnalyzeService;

	
	@RequestMapping(value = "/adm/queryaudienceanalyze", method = RequestMethod.POST, headers = "Accept=application/json;charset=UTF-8")
	public String queryAudienceanAlyze(
			HttpServletRequest request,
			ModelAndView modelAndView,
			String keyType,
			String userType,
			String source,
			String recordDate,
			String keyId
			) {
		JSONObject result = new JSONObject();
		try {
			 
			StringBuffer hql = new StringBuffer(" from AdmCategoryAudienceAnalyze where 1=1 ");
			
			if (StringUtils.isNotBlank(keyType)){
				hql.append(" and keyType  = '"+keyType+"' ");
			}
			
			if (StringUtils.isNotBlank(userType)){
				if (StringUtils.equals("memid", userType)){
					hql.append(" and userType  = '"+userType+"' ");	
				}
				if (StringUtils.equals("uuid", userType)){
					hql.append(" and userType  = '"+userType+"' ");	
				}
				
			}
			
			if (StringUtils.isNotBlank(source)){
				if (StringUtils.equals("24h", source)){
					hql.append(" and source  = '"+source+"' ");	
				}
				if (StringUtils.equals("ruten", source)){
					hql.append(" and source  = '"+source+"' ");	
				}
				if (StringUtils.equals("adclick", source)){
					hql.append(" and source  = '"+source+"' ");	
				}
			}
			
			if (StringUtils.isNotBlank(recordDate)){
				String[] str=null;
				
				if(recordDate.contains("/")){
					str =recordDate.split("/");
				}
				
				if(recordDate.contains("-")){
					str =recordDate.split("-");
				}
				
				String recordDateNew=str[2]+"-"+str[0]+"-"+str[1];
				hql.append(" and recordDate = '"+recordDateNew+"' ");
			}
			
			if (StringUtils.isNotBlank(keyId)){
				hql.append(" and keyId = '"+keyId+"' ");
			}
			
			
			List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.findByPage(hql.toString(), 1, 200);
			
			
			//來源為all
			Map<String,AdmCategoryAudienceAnalyze> map = new HashMap<>();
			if(StringUtils.isNotBlank(source) && source.equals("all")){
				if(StringUtils.isNotBlank(keyType) && StringUtils.isNotBlank(userType)){
					for (AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze : list) {
						if(!admCategoryAudienceAnalyze.getKeyType().equals(keyType)){
							continue;
						}
						if(map.containsKey(admCategoryAudienceAnalyze.getKeyId())){
							AdmCategoryAudienceAnalyze admCategoryAudienceAnalyzeData = map.get(admCategoryAudienceAnalyze.getKeyId());
							int count = admCategoryAudienceAnalyzeData.getKeyCount();
							count = count + admCategoryAudienceAnalyze.getKeyCount();
							admCategoryAudienceAnalyzeData.setKeyCount(count);
							map.put(admCategoryAudienceAnalyze.getKeyId(), admCategoryAudienceAnalyzeData);
						}else{
							map.put(admCategoryAudienceAnalyze.getKeyId(), admCategoryAudienceAnalyze);
						}
					}
				}else{
					for (AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze : list) {
						if(map.containsKey(admCategoryAudienceAnalyze.getKeyId())){
							AdmCategoryAudienceAnalyze admCategoryAudienceAnalyzeData = map.get(admCategoryAudienceAnalyze.getKeyId());
							int count = admCategoryAudienceAnalyzeData.getKeyCount();
							count = count + admCategoryAudienceAnalyze.getKeyCount();
							admCategoryAudienceAnalyzeData.setKeyCount(count);
							map.put(admCategoryAudienceAnalyze.getKeyId(), admCategoryAudienceAnalyzeData);
						}else{
							map.put(admCategoryAudienceAnalyze.getKeyId(), admCategoryAudienceAnalyze);
						}
					}
				}
			}
			
			List<AdmCategoryAudienceAnalyze> listAll = new ArrayList<>(); 
			for (Map.Entry<String, AdmCategoryAudienceAnalyze> entry : map.entrySet()){
				AdmCategoryAudienceAnalyze admCategoryAudienceAnalyze = new AdmCategoryAudienceAnalyze();
				admCategoryAudienceAnalyze.setId(-1);
				admCategoryAudienceAnalyze.setCreateDate(entry.getValue().getCreateDate());
				admCategoryAudienceAnalyze.setKeyCount(entry.getValue().getKeyCount());
				admCategoryAudienceAnalyze.setKeyId(entry.getValue().getKeyId());
				admCategoryAudienceAnalyze.setKeyName(entry.getValue().getKeyName());
				if(StringUtils.isBlank(keyType)){
					admCategoryAudienceAnalyze.setKeyType("All");	
				}else{
					admCategoryAudienceAnalyze.setKeyType(entry.getValue().getKeyType());	
				}
				
				admCategoryAudienceAnalyze.setRecordDate(entry.getValue().getRecordDate());
				admCategoryAudienceAnalyze.setUserType(StringUtils.isNotBlank(userType) ? userType : "All");
				admCategoryAudienceAnalyze.setSource("All");
				listAll.add(admCategoryAudienceAnalyze);
			}
			if(listAll.size() > 0){
				result.put("result", "SUCCESS");
				result.put("admCategoryAudienceAnalyzeList", listAll);
				result.put("pageSize", listAll.size());
				return result.toString();
			}
			
			
			result.put("result", "SUCCESS");
			result.put("admCategoryAudienceAnalyzeList", list);
			result.put("pageSize", list.size());
			return result.toString();
	        
		} catch (Exception e) {
			e.printStackTrace();
			result.put("result", "FAIL");
			result.put("msg", "system error");
			return result.toString();
		}
	}
	
	@RequestMapping(value = "/adm/queryrecordcount", method = RequestMethod.POST, headers = "Accept=application/json;charset=UTF-8")
	public String queryRecordCount(
			HttpServletRequest request,
			ModelAndView modelAndView,
			String keyType,
			String userType,
			String source,
			String recordDate,
			String keyId
			) {
		JSONObject result = new JSONObject();
		try {
			 
			StringBuffer hql = new StringBuffer(" from AdmCategoryAudienceAnalyze where 1=1 ");
			
			if (StringUtils.isNotBlank(keyType)){
				hql.append(" and keyType  = '"+keyType+"' ");
			}
			
			if (StringUtils.isNotBlank(userType)){
				if (StringUtils.equals("memid", userType)){
					hql.append(" and userType  = '"+userType+"' ");	
				}
				if (StringUtils.equals("uuid", userType)){
					hql.append(" and userType  = '"+userType+"' ");	
				}
				
			}
			
			if (StringUtils.isNotBlank(source)){
				if (StringUtils.equals("24h", source)){
					hql.append(" and source  = '"+source+"' ");	
				}
				if (StringUtils.equals("ruten", source)){
					hql.append(" and source  = '"+source+"' ");	
				}
				if (StringUtils.equals("adclick", source)){
					hql.append(" and source  = '"+source+"' ");	
				}
			}
			
			if (StringUtils.isNotBlank(recordDate)){
				hql.append(" and recordDate = '"+recordDate+"' ");
			}
			
			if (StringUtils.isNotBlank(keyId)){
				hql.append(" and keyId = '"+keyId+"' ");
			}
			
			
			int listSize=admCategoryAudienceAnalyzeService.rowCount(hql.toString());
			
			result.put("result", "SUCCESS");
			result.put("pageSize", listSize);
			return result.toString();
	        
		} catch (Exception e) {
			e.printStackTrace();
			result.put("result", "FAIL");
			result.put("msg", "system error");
			return result.toString();
		}
	}
	
	
}
