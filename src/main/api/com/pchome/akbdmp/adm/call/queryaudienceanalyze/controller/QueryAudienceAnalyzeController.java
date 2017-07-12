package com.pchome.akbdmp.adm.call.queryaudienceanalyze.controller;

import java.util.List;

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
				hql.append(" and recordDate = '"+recordDate+"' ");
			}
			
			if (StringUtils.isNotBlank(keyId)){
				hql.append(" and keyId = '"+keyId+"' ");
			}
			
			
			List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.findByPage(hql.toString(), 1, 200);
			
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
