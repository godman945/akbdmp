package com.pchome.hadoopdmp.mongo.db.service.userdetail;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.mongo.db.dao.userdetail.IUserDetailDAO;
import com.pchome.hadoopdmp.mongo.db.service.base.BaseService;

@Service
public class UserDetailService extends BaseService<UserDetailMongoBean> implements IUserDetailService {

	private static final long serialVersionUID = 1L;

	final static Logger log = Logger.getLogger(UserDetailService.class.getName());

	@Autowired
	IUserDetailDAO classCountDAO;

	public UserDetailMongoBean saveOrUpdate(UserDetailMongoBean userDetailMongoBean) throws Exception {
		return classCountDAO.saveOrUpdate(userDetailMongoBean);
	}
	
	
	public UserDetailMongoBean findUserId(String userId) throws Exception{
		return classCountDAO.findUserId(userId);
	}
}