package com.pchome.akbdmp.mongo.db.dao.classcount;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.pchome.akbdmp.api.data.enumeration.ClassCountMongoDBEnum;
import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.mongo.db.dao.base.BaseDAO;

@Repository
public class ClassCountDAO extends BaseDAO<ClassCountMongoBean> implements IClassCountDAO {

	private static final long serialVersionUID = 1L;

	@Autowired
	MongoOperations mongoOperations;

	public ClassCountMongoBean saveOrUpdate(ClassCountMongoBean classCountMongoBean) throws Exception {
		mongoOperations.save(classCountMongoBean);
		return classCountMongoBean;
	}

	public ClassCountMongoBean findUserId(String userId) throws Exception {
		Query query = new Query(Criteria.where(ClassCountMongoDBEnum.USER_ID.getKey()).is(userId));
		return mongoOperations.findOne(query, ClassCountMongoBean.class);
	}
}
