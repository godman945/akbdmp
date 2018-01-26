package com.pchome.hadoopdmp.mongo.db.dao.userdetail;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.api.data.enumeration.ClassCountMongoDBEnum;
import com.pchome.hadoopdmp.data.mongo.pojo.UserDetailMongoBean;
import com.pchome.hadoopdmp.mongo.db.dao.base.BaseDAO;

@Repository
public class UserDetailDAO extends BaseDAO<UserDetailMongoBean> implements IUserDetailDAO {

	private static final long serialVersionUID = 1L;

	@Autowired
	MongoOperations mongoOperations;

	public UserDetailMongoBean saveOrUpdate(UserDetailMongoBean classCountMongoBean) throws Exception {
		mongoOperations.save(classCountMongoBean);
		return classCountMongoBean;
	}

	public UserDetailMongoBean findUserId(String userId) throws Exception {
		Query query = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(userId));
		return mongoOperations.findOne(query, UserDetailMongoBean.class);
	}
}
