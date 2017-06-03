package com.pchome.akbdmp.mongo.db.dao.classcount;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Repository;

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

}
