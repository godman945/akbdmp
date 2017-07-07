package com.pchome.akbdmp.mysql.db.dao.user;


import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.data.mysql.pojo.AdmUser;
import com.pchome.akbdmp.mysql.db.dao.base.IBaseDAO;

@Repository
public interface IAdmUserDao extends IBaseDAO<AdmUser, Integer>{

	public boolean checkUser(String userId,String password) throws Exception;
}
