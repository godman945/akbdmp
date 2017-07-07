package com.pchome.akbdmp.mysql.db.service.user;


import org.springframework.stereotype.Service;

import com.pchome.akbdmp.data.mysql.pojo.AdmUser;
import com.pchome.akbdmp.mysql.db.service.base.IBaseService;

@Service
public interface IAdmUserService extends IBaseService<AdmUser, Integer>{

	public boolean checkUser(String userId,String password) throws Exception;

	
}
