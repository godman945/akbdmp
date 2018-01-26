package com.pchome.hadoopdmp.mongo.db.dao.userdetail;

import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.mongo.db.dao.base.IBaseDAO;

public interface IUserDetailDAO extends IBaseDAO<UserDetailMongoBean> {


	public UserDetailMongoBean saveOrUpdate(UserDetailMongoBean userDetailMongoBean) throws Exception;

	public UserDetailMongoBean findUserId(String userId) throws Exception;
}
