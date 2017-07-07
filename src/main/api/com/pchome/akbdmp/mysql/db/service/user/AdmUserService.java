package com.pchome.akbdmp.mysql.db.service.user;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pchome.akbdmp.data.mysql.pojo.AdmUser;
import com.pchome.akbdmp.mysql.db.dao.user.IAdmUserDao;
import com.pchome.akbdmp.mysql.db.service.base.BaseService;

@Service
public class AdmUserService extends BaseService<AdmUser, Integer> implements IAdmUserService{
	@Autowired
	private IAdmUserDao admUserDao;
	
	public boolean checkUser(String userId,String password) throws Exception{
		return admUserDao.checkUser(userId,password);
	}
}
