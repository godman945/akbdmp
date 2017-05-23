package com.pchome.akbdmp.api.data.check;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.api.data.enumeration.DmpAdShowLimitParamaterEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpApiReturnCodeEnum;
import com.pchome.akbdmp.api.data.returndata.ReturnData;

@Component
@Scope("request")
public class CheckAdSowLimit extends ACheckData {

	Log log = LogFactory.getLog(CheckAdSowLimit.class);

	@Autowired
	private ReturnData returnData;

	
	public Object checkData(JSONObject json) throws Exception {
		String[] adLimitStr = (String[]) json.get(DmpAdShowLimitParamaterEnum.AD_KEY.getKey());
		if(adLimitStr.length < 1){
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E001.getCode());
			returnData.setResult(DmpApiReturnCodeEnum.API_CODE_E001.getContent());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E001.isStatus());
			return returnData;
		}
		returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_S001.isStatus());
		return returnData;
	}
}