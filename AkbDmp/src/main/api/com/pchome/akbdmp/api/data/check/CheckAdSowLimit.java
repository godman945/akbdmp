package com.pchome.akbdmp.api.data.check;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.pchome.akbdmp.api.data.enumeration.DmpAdShowLimitParamaterEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpApiReturnCodeEnum;
import com.pchome.akbdmp.api.data.returndata.ReturnData;

import net.minidev.json.JSONArray;

@Component
@Scope("request")
public class CheckAdSowLimit extends ACheckData {

	Log log = LogFactory.getLog(CheckAdSowLimit.class);

	@Autowired
	private ReturnData returnData;

	@Autowired
	private Configuration jsonpathConfiguration;
	
	public Object checkData(JSONObject json) throws Exception {
		String adLimitStr = json.getString(DmpAdShowLimitParamaterEnum.AD_KEY.getKey());
		if(JsonPath.using(jsonpathConfiguration).parse(adLimitStr).read(DmpAdShowLimitParamaterEnum.AD_KEY.getKey()) == null){
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E001.getCode());
			returnData.setResult(DmpApiReturnCodeEnum.API_CODE_E001.getContent());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E001.isStatus());
			return returnData;
		}
		JSONArray showFrequencyKeyArray = JsonPath.using(jsonpathConfiguration).parse(adLimitStr).read(DmpAdShowLimitParamaterEnum.AD_KEY.getKey());
		if(showFrequencyKeyArray.size() == 0){
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E002.getCode());
			returnData.setResult(DmpApiReturnCodeEnum.API_CODE_E002.getContent());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E002.isStatus());
			return returnData;
		}
		returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_S001.isStatus());
		returnData.setResult(showFrequencyKeyArray);
		return returnData;

	}
}