package com.pchome.hadoopdmp.mongo.db.service.userdetail;

import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.mongo.db.service.base.IBaseService;

public interface IUserDetailService extends IBaseService<UserDetailMongoBean>{
	
	public UserDetailMongoBean saveOrUpdate(UserDetailMongoBean userDetailMongoBean) throws Exception;
	
	public UserDetailMongoBean findUserId(String userId) throws Exception;
	
}
