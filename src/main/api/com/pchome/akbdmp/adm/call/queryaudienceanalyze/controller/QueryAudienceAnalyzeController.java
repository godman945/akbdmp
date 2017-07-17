package com.pchome.akbdmp.adm.call.queryaudienceanalyze.controller;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
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
			String keyId,
			String page,
			String pageSize
			) {
		JSONObject result = new JSONObject();
		
		try {
			
			if (StringUtils.isBlank(page)){
				page="1";
			}
			 
			//受眾類型和來源都不為all
			if ( (!StringUtils.equals("all", userType)) && (!StringUtils.equals("all", source))){
			
				
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
					
					String recordDateNew=str[2]+"/"+str[0]+"/"+str[1];
					hql.append(" and recordDate = '"+recordDateNew.trim()+"' ");
				}
				
				if (StringUtils.isNotBlank(keyId)){
					hql.append(" and keyId = '"+keyId+"' ");
				}
				
				//拿分頁size
				List<AdmCategoryAudienceAnalyze> list=admCategoryAudienceAnalyzeService.findByPage(hql.toString(), Integer.parseInt(page) , Integer.parseInt(pageSize));
	
				//拿全部的size
				int listSize=admCategoryAudienceAnalyzeService.rowCount(hql.toString());
	
				result.put("result", "SUCCESS");
				result.put("admCategoryAudienceAnalyzeList", list);
				result.put("pageSize", listSize);
			}
			
			//受眾類型或來源為all
			if ( (StringUtils.equals("all", userType)) || (StringUtils.equals("all", source))){
				
				int startRange = (Integer.parseInt(page)-1)*Integer.parseInt(pageSize);
				int endRange = Integer.parseInt(page)*Integer.parseInt(pageSize);
				
				StringBuffer sql = new StringBuffer(" SELECT id, record_date, key_id, key_name, key_type, user_type, source, SUM( key_count ) AS key_count ");
//				System.out.println("startRange : "+startRange);
//				System.out.println("endRange : "+endRange);
				
				//userType和Source皆為all
				if ( (StringUtils.equals("all", userType)) && (StringUtils.equals("all", source))){
					sql.append(" FROM adm_category_audience_analyze ");
					sql.append(" WHERE 1=1 ");
					
					if (StringUtils.isNotBlank(keyId)){
						sql.append(" and key_id = '"+keyId+"' ");
					}
					
					if (StringUtils.isNotBlank(keyType)){
						sql.append(" and key_type = '"+keyType+"' ");
					}
					sql.append(" GROUP BY record_date, key_id, key_name, key_type ASC ");
				}
				
				//userType不為all 和  Source為all
				if ( !(StringUtils.equals("all", userType)) && (StringUtils.equals("all", source))){
					sql.append(" FROM adm_category_audience_analyze ");
					sql.append(" WHERE 1=1 ");
					
					if (StringUtils.isNotBlank(keyId)){
						sql.append(" and key_id = '"+keyId+"' ");
					}
					
					if (StringUtils.isNotBlank(keyType)){
						sql.append(" and key_type = '"+keyType+"' ");
					}
					
					if (StringUtils.isNotBlank(userType)){
						sql.append(" and user_type = '"+userType+"' ");
					}
					sql.append(" GROUP BY record_date, key_id, key_name, key_type , user_type ASC ");
				}
				
				//userType為all 和  Source不為all
				if ( (StringUtils.equals("all", userType)) && (!StringUtils.equals("all", source))){
					sql.append(" FROM adm_category_audience_analyze ");
					sql.append(" WHERE 1=1 ");
					
					if (StringUtils.isNotBlank(keyId)){
						sql.append(" and key_id = '"+keyId+"' ");
					}
					
					if (StringUtils.isNotBlank(keyType)){
						sql.append(" and key_type = '"+keyType+"' ");
					}
					
					if (StringUtils.isNotBlank(source)){
						sql.append(" and source = '"+source+"' ");
					}
				
					sql.append(" GROUP BY record_date, key_id, key_name, key_type , source ASC ");
				}
				
				StringBuffer sqlByPage = new StringBuffer(sql);
				sqlByPage.append(" LIMIT ");
				sqlByPage.append(startRange);
				sqlByPage.append(" , ");
				sqlByPage.append(endRange) ;
				
				
				List<Object> listByPage = admCategoryAudienceAnalyzeService.sqlFindByPage(sqlByPage.toString());
				
				List<AdmCategoryAudienceAnalyze> audienceAnalyzeList =new ArrayList<>();
				
				for (Object obj : listByPage) {
					Object[] objArray= (Object[])obj;
					
					AdmCategoryAudienceAnalyze audienceAnalyzeObj =new AdmCategoryAudienceAnalyze();
					audienceAnalyzeObj.setId((Integer)objArray[0]);
					audienceAnalyzeObj.setRecordDate((String)objArray[1]);
					audienceAnalyzeObj.setKeyId((String)objArray[2]);
					audienceAnalyzeObj.setKeyName((String)objArray[3]);
					audienceAnalyzeObj.setKeyType((String)objArray[4]);
					audienceAnalyzeObj.setUserType((StringUtils.equals("all", userType))?"All":(String)objArray[5]); 
					audienceAnalyzeObj.setSource((StringUtils.equals("all", source))?"All":(String)objArray[6]);
					audienceAnalyzeObj.setKeyCount(((BigDecimal) objArray[7]).intValue());

					audienceAnalyzeList.add(audienceAnalyzeObj);
					
				}
				
				//拿全部的size
				int allListSize=admCategoryAudienceAnalyzeService.sqlRowCount(sql.toString());
				
				result.put("result", "SUCCESS");
				result.put("admCategoryAudienceAnalyzeList", audienceAnalyzeList);
				result.put("pageSize", allListSize);
			}
			
			return result.toString();
	        
		} catch (Exception e) {
			e.printStackTrace();
			result.put("result", "FAIL");
			result.put("msg", "system error");
			return result.toString();
		}
	}
	
//	@RequestMapping(value = "/adm/queryrecordcount", method = RequestMethod.POST, headers = "Accept=application/json;charset=UTF-8")
//	public String queryRecordCount(
//			HttpServletRequest request,
//			ModelAndView modelAndView,
//			String keyType,
//			String userType,
//			String source,
//			String recordDate,
//			String keyId
//			) {
//		JSONObject result = new JSONObject();
//		try {
//			 
//			StringBuffer hql = new StringBuffer(" from AdmCategoryAudienceAnalyze where 1=1 ");
//			
//			if (StringUtils.isNotBlank(keyType)){
//				hql.append(" and keyType  = '"+keyType+"' ");
//			}
//			
//			if (StringUtils.isNotBlank(userType)){
//				if (StringUtils.equals("memid", userType)){
//					hql.append(" and userType  = '"+userType+"' ");	
//				}
//				if (StringUtils.equals("uuid", userType)){
//					hql.append(" and userType  = '"+userType+"' ");	
//				}
//				
//			}
//			
//			if (StringUtils.isNotBlank(source)){
//				if (StringUtils.equals("24h", source)){
//					hql.append(" and source  = '"+source+"' ");	
//				}
//				if (StringUtils.equals("ruten", source)){
//					hql.append(" and source  = '"+source+"' ");	
//				}
//				if (StringUtils.equals("adclick", source)){
//					hql.append(" and source  = '"+source+"' ");	
//				}
//			}
//			
//			if (StringUtils.isNotBlank(recordDate)){
//				hql.append(" and recordDate = '"+recordDate+"' ");
//			}
//			
//			if (StringUtils.isNotBlank(keyId)){
//				hql.append(" and keyId = '"+keyId+"' ");
//			}
//			
//			
//			int listSize=admCategoryAudienceAnalyzeService.rowCount(hql.toString());
//			
//			result.put("result", "SUCCESS");
//			result.put("pageSize", listSize);
//			return result.toString();
//	        
//		} catch (Exception e) {
//			e.printStackTrace();
//			result.put("result", "FAIL");
//			result.put("msg", "system error");
//			return result.toString();
//		}
//	}
	
	
}
